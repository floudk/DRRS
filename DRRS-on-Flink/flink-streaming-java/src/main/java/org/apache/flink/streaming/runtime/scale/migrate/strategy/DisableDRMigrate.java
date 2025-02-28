package org.apache.flink.streaming.runtime.scale.migrate.strategy;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.io.ScaleCommListener;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.io.message.barrier.ConfirmBarrier;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.io.message.local.StateBuffer;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DisableDRMigrate extends FluidMigrate{

    public Map<TriggerBarrier, Integer> barrierCount = new HashMap<>();

    private final IndexedInputGate[] inputGates;
    private final Set<InputChannelInfo> blockedChannels = new HashSet<>();

    private final Map<Integer,CompletableFuture<Void>> alignmentFutures = new ConcurrentHashMap<>();


    public DisableDRMigrate(
            StreamOperator mainOperator,
            ScaleCommOutAdapter scaleCommOutAdapter,
            ScaleCommListener scaleCommListener,
            ScalingContext scalingContext,
            IndexedInputGate[] inputGates,
            BiConsumer<RunnableWithException,String> poisonMailbox,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling) {
        super(mainOperator, scaleCommOutAdapter, scaleCommListener, scalingContext, inputGates, poisonMailbox, recordProcessorInScaling);
        this.inputGates = inputGates;
    }

    @Override
    public void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo) throws IOException {

        Map<Integer, List<Integer>> sourceTaskWithInKeys = tb.getInKeys(subtaskIndex);
        Map<Integer, List<Integer>> targetTaskWithOutKeys = tb.getOutKeys(subtaskIndex);
        if (sourceTaskWithInKeys.isEmpty() && targetTaskWithOutKeys.isEmpty()){
            // no migration needed
            unblockChannel(channelInfo);
            return;
        }

        barrierCount.put(tb, barrierCount.getOrDefault(tb, 0) + 1);
        LOG.info("Received new trigger barrier {} {}/{} from channel {}.",  tb, barrierCount.get(tb), targetChannelCount, channelInfo);

        if (barrierCount.get(tb) == 1){
            // only react to the first barrier

            // main thread: prepare for receiving events and buffers
            scalingContext.subscale(sourceTaskWithInKeys, targetTaskWithOutKeys, tb.subscaleID);

            migrationBuffer.subscale(sourceTaskWithInKeys);
            // async request state migration in
            intraSubscaleKeyOrderSelector.registerSelectorForSubscale(
                    tb.subscaleID,
                    sourceTaskWithInKeys,
                    tb.involvedKeys);
            requestStatesAsync(sourceTaskWithInKeys,tb.subscaleID);
        }

        if (targetTaskWithOutKeys.isEmpty()){
            unblockChannel(channelInfo);
        }else{
            blockedChannels.add(channelInfo);
            if (barrierCount.get(tb) == targetChannelCount){
                // allow any migration to proceed
                for (List<Integer> outKeys : targetTaskWithOutKeys.values()){
                    for (Integer outKey : outKeys){
                        alignmentFutures.computeIfAbsent(outKey, k -> new CompletableFuture<>())
                                .complete(null);
                    }
                }

                for (InputChannelInfo blockedChannel : blockedChannels) {
                    unblockChannel(blockedChannel);
                }
                blockedChannels.clear();
            }
        }

    }

    @Override
    public void processEvent(ScaleEvent event, Channel channel){
        if (event instanceof ScaleEvent.StateRequestEvent) {
            LOG.info("{}: Received request state transfer event {}.", taskName, event);
            final ScaleEvent.StateRequestEvent stateRequestEvent = (ScaleEvent.StateRequestEvent) event;
            scaleCommOutAdapter.onRequestedStatesReceived(stateRequestEvent.subscaleID, stateRequestEvent.eventSenderIndex,channel);
            // start migrating states
            migrateStatesAsync(stateRequestEvent);
        }else if (event instanceof ScaleEvent.AcknowledgeStates) {
            // notify the state has been transmitted
            final int keyGroupIndex = ((ScaleEvent.AcknowledgeStates) event).ackedKeyGroup;
            LOG.info("{}: Received acknowledge state transfer event {}.", taskName, keyGroupIndex);
            scaleMailConsumer.accept(
                    () -> {
                        scalingContext.removeFromOutPendingKeys(List.of(keyGroupIndex));
                        scalingContext.notifyAllConfirmBarriersRerouted(Set.of(keyGroupIndex));
                        },
                    "Notify state transmitted"
            );
            CompletableFuture<Integer> transmitComplete = stateTransmitCompletes.remove(keyGroupIndex);
            checkNotNull(transmitComplete, "KeyGroupIndex " + keyGroupIndex + " is not found.");
            transmitComplete.complete(-1);
        }else{
            throw new IllegalArgumentException("Unknown event type: " + event);
        }
    }

    @Override
    public <T> void processRecord(
            StreamRecord<T> record,
            InputChannelInfo channelInfo,
            int keyGroupIndex,
            Counter numRecordsIn,
            Input<T> input) throws Exception {

        checkNotNull(channelInfo, "ChannelInfo is null.");

        if (scalingContext.isIncomingKey(keyGroupIndex)){
            if (scalingContext.isLocalKeyGroup(keyGroupIndex)){
                directProcess(record, numRecordsIn, input);
            }else{
                if (!migrationBuffer.cacheRecord(record, channelInfo, keyGroupIndex)){
                    LOG.info("{}: Full migration buffer, suspend processing at {}.", taskName, System.currentTimeMillis());
                    availabilityHelper.resetUnavailable();
                }
            }
        }else if (scalingContext.isLocalKeyGroup(keyGroupIndex)){
            directProcess(record, numRecordsIn, input);
        }else{
            int subscaleID = scalingContext.getSubscaleID(keyGroupIndex);
            reroute(record, keyGroupIndex, subscaleID);
        }
    }

    @Override
    protected void migrateStatesAsync(ScaleEvent.StateRequestEvent request){
        List<CompletableFuture<Void>> neededAlignmentFutures = request.requestedStates.stream()
                .map(key -> alignmentFutures.computeIfAbsent(key, k -> new CompletableFuture<>()))
                .collect(Collectors.toList());
        FutureUtils.waitForAll(neededAlignmentFutures)
                .thenRunAsync(()->super.migrateStatesAsync(request));
    }

    @Override
    public void processBuffer(ScaleBuffer buffer, int fromTaskIndex){
        if (buffer instanceof StateBuffer){
            // merge state
            final StateBuffer stateBuffer = (StateBuffer) buffer;
            int keyGroupIndex = (int) stateBuffer.outgoingManagedKeyedState.keySet().iterator().next();
            CompletableFuture.runAsync(
                    () -> {
                        LOG.info("{}: Sending acknowledge {} to subtask {}.",
                                taskName,
                                keyGroupIndex,
                                fromTaskIndex);
                        scaleCommOutAdapter.ackStateReceived(
                                fromTaskIndex,
                                keyGroupIndex,
                                scalingContext.getSubscaleID(keyGroupIndex),-1);
                    });
            mergeState(stateBuffer, fromTaskIndex);
        } else{
            throw new IllegalArgumentException("Unknown buffer type: " + buffer);
        }
    }

    @Override
    protected void mergeState(StateBuffer stateBuffer, int fromTaskIndex){
        checkNotNull(mainOperator, "Main operator is not set yet in task " + taskName);
        scaleMailConsumer.accept(
                () -> {
                    LOG.info("{}: merge state {} in Thread {}",
                            taskName,
                            stateBuffer,
                            Thread.currentThread().getName());
                    mainOperator.mergeState(stateBuffer);
                    scalingContext.removeFromInPendingKeys(stateBuffer.outgoingManagedKeyedState.keySet(),fromTaskIndex);

                    if (migrationBuffer.notifyStateMergedWithDisabledDR(
                            stateBuffer.outgoingManagedKeyedState.keySet(),
                            scalingContext,
                            fromTaskIndex)){
                        LOG.info("{} merge state completed and some cached records are processed, "
                                        + "reset availability",
                                taskName);
                        CompletableFuture toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        toNotify.complete(null);
                    }
                },
                "Merge states"
        );
    }

    @Override
    public void onConfirmBarrier(ConfirmBarrier cb, InputChannelInfo channelInfo) {
        throw new IllegalStateException("Received confirm barrier in DisableDRMigrate");
    }

    private void unblockChannel(InputChannelInfo channelInfo) throws IOException {
        inputGates[channelInfo.getGateIdx()].resumeConsumption(channelInfo);
    }

}
