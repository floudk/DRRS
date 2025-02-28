package org.apache.flink.streaming.runtime.scale.migrate.strategy;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.coordinator.SubtaskScaleResourceReleaser;
import org.apache.flink.runtime.scale.io.ScaleCommListener;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.io.message.local.StateBuffer;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.scale.migrate.MigrationBuffer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


/**
 * Fluid migration will split the whole migrated state into multiple small parts.
 * By which, we can migra\te the state and concurrently process the rest of the state.
 */

public class FluidMigrate extends MigrateStrategy {

    protected final MigrationBuffer migrationBuffer;

    protected final Map<Integer, CompletableFuture<Void>> stateTransmitCompletes = new ConcurrentHashMap<>();
    protected final Map<Integer, List<Integer>> stateTransmitCache = new HashMap<>();

    protected final Map<TriggerBarrier,Integer> pendingTriggerBarriers = new HashMap<>();

    private final AtomicInteger outBufferID = new AtomicInteger(0);

    private final IndexedInputGate[] inputGates;
    private final Set<InputChannelInfo> blockedChannels = new HashSet<>();

    public FluidMigrate(
            StreamOperator mainOperator,
            ScaleCommOutAdapter scaleCommOutAdapter,
            ScaleCommListener scaleCommListener,
            ScalingContext scalingContext,
            IndexedInputGate[] inputGates,
            BiConsumer<RunnableWithException,String> poisonMailbox,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling) {
        this(
                mainOperator,
                scaleCommOutAdapter,
                scaleCommListener,
                scalingContext,
                inputGates,
                poisonMailbox,
                recordProcessorInScaling,
                true,
                true);
    }

    public FluidMigrate(
            StreamOperator mainOperator,
            ScaleCommOutAdapter scaleCommOutAdapter,
            ScaleCommListener scaleCommListener,
            ScalingContext scalingContext,
            IndexedInputGate[] inputGates,
            BiConsumer<RunnableWithException,String> poisonMailbox,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling,
            boolean enableMigrationBuffer,
            boolean enableRerouting) {
        super(
                mainOperator,
                scaleCommOutAdapter,
                scaleCommListener,
                scalingContext,
                Arrays.stream(inputGates).map(IndexedInputGate::getNumberOfInputChannels).mapToInt(Integer::intValue).sum(),
                poisonMailbox,
                recordProcessorInScaling);

        this.inputGates = inputGates;

        this.migrationBuffer = new MigrationBuffer();

        availabilityHelper.resetAvailable(); // reset the availability in the beginning
    }

    @Override
    public void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo) throws IOException {

        Map<Integer, List<Integer>> sourceTaskWithInKeys = tb.getInKeys(subtaskIndex);
        Map<Integer, List<Integer>> targetTaskWithOutKeys = tb.getOutKeys(subtaskIndex);

        if (!pendingTriggerBarriers.containsKey(tb)){
            pendingTriggerBarriers.put(tb, 1);
            scalingContext.subscale(sourceTaskWithInKeys, targetTaskWithOutKeys);
            requestStatesAsync(sourceTaskWithInKeys);
        }else{
            pendingTriggerBarriers.compute(tb, (k, v) -> v + 1);
        }

        if (targetTaskWithOutKeys.isEmpty()){
            unblockChannel(channelInfo);
        }else{
            blockedChannels.add(channelInfo);
            if (pendingTriggerBarriers.get(tb) == targetChannelCount) {
                Set<Integer> allMigratingOutKeys = targetTaskWithOutKeys.values().stream().flatMap(List::stream)
                        .collect(Collectors.toSet());

                LOG.info("{}: successfully aligned trigger barrier for out keys{}.", taskName, allMigratingOutKeys);
                // if CompletableFuture exists, then complete it, otherwise, create a new completed future

                synchronized (prepareStateTransmitCompletes){
                    allMigratingOutKeys.forEach(key -> {
                        if (prepareStateTransmitCompletes.containsKey(key)) {
                            prepareStateTransmitCompletes.get(key).complete(null);
                        }else{
                            prepareStateTransmitCompletes.put(key, CompletableFuture.completedFuture(null));
                        }
                    });
                }

                for (InputChannelInfo blockedChannel : blockedChannels) {
                    unblockChannel(blockedChannel);
                }
                blockedChannels.clear();
            }
        }

    }

    private void unblockChannel(InputChannelInfo channelInfo) throws IOException {
        inputGates[channelInfo.getGateIdx()].resumeConsumption(channelInfo);
    }



    // ------------------ state migration ------------------
    private CompletableFuture<Void> requestStatesAsync(Map<Integer, List<Integer>> sourceTaskWithInKeys) {
        // async request states
        return CompletableFuture.runAsync(() -> {
            sourceTaskWithInKeys.forEach((sourceSubtask, inKeys) -> {
                ScaleEvent requestStateEvent = new ScaleEvent.RequestStates(subtaskIndex, inKeys);
                try {
                    LOG.info("{}: Sending request state event to subtask {}.", taskName, sourceSubtask);
                    scaleCommOutAdapter.sendEvent(sourceSubtask, requestStateEvent);
                } catch (Exception e) {
                    LOG.error("Failed to send request state event to subtask {}.", sourceSubtask, e);
                    throw new RuntimeException(e);
                }
            });
        });
    }

    @Override
    public void processEvent(ScaleEvent event){
        if (event instanceof ScaleEvent.RequestStates) {
            final ScaleEvent.RequestStates requestStates = (ScaleEvent.RequestStates) event;
            LOG.info("{}: Received request state event {} from subtask {}.",
                    taskName,
                    requestStates.requestedStates,
                    requestStates.eventSenderIndex);
            // start migrating states
            migrateStatesAsync(requestStates.eventSenderIndex, requestStates.requestedStates);
        }else if (event instanceof ScaleEvent.AcknowledgeStates) {
            // notify the state has been transmitted
            final int stateBufferID = ((ScaleEvent.AcknowledgeStates) event).stateBufferID;
            LOG.info("{}: Received acknowledge state transfer event {}.", taskName, stateBufferID);
            scaleMailConsumer.accept(
                    () -> scalingContext.removeFromOutPendingKeys(stateTransmitCache.remove(stateBufferID)),
                    "Notify state transmitted"
            );
            CompletableFuture<Void> transmitComplete = stateTransmitCompletes.remove(stateBufferID);
            checkNotNull(transmitComplete, "State buffer ID " + stateBufferID + " is not found.");
            transmitComplete.complete(null);
        }else{
            throw new IllegalArgumentException("Unknown event type: " + event);
        }
    }

    @Override
    public void processBuffer(ScaleBuffer buffer, int fromTaskIndex){
        if (buffer instanceof StateBuffer){
            // merge state
            final StateBuffer stateBuffer = (StateBuffer) buffer;
            final int bufferID = stateBuffer.bufferID;
            CompletableFuture.runAsync(
                    () -> {
                        try {
                            scaleCommOutAdapter.sendEvent(
                                    fromTaskIndex,
                                    new ScaleEvent.AcknowledgeStates(subtaskIndex, bufferID));
                        } catch (InterruptedException e) {
                            LOG.error("Failed to send acknowledge state event.", e);
                            throw new RuntimeException(e);
                        }
                    });
            mergeState(stateBuffer);
        } else{
            throw new IllegalArgumentException("Unknown buffer type: " + buffer);
        }
    }

    @Override
    protected void migrateStatesAsync(int targetTaskIndex,List<Integer> requestedStates) {
        CompletableFuture.runAsync(() -> {
            try {
                final int stateBufferID = outBufferID.getAndIncrement();
                CompletableFuture<Void> transmitComplete = new CompletableFuture<>();
                stateTransmitCompletes.put(stateBufferID, transmitComplete);

                stateTransmitCache.put(stateBufferID, requestedStates);
                CompletableFuture<Void> adapterConsumeFuture = new CompletableFuture<>();
                collectAndMigrateStates(
                        targetTaskIndex, requestedStates, stateBufferID, adapterConsumeFuture);
                // wait for the state to be transmitted
                transmitComplete.join();
            } catch (Exception e) {
                LOG.error("Failed to migrate state for keygroup {}.", requestedStates, e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected void mergeState(StateBuffer stateBuffer){
        checkNotNull(mainOperator, "Main operator is not set yet in task " + taskName);
        scaleMailConsumer.accept(
                () -> {
                    LOG.info("{}: merge state {} in Thread {}",
                            taskName,
                            stateBuffer,
                            Thread.currentThread().getName());
                    mainOperator.mergeState(stateBuffer);
                    scalingContext.removeFromInPendingKeys(stateBuffer.outgoingManagedKeyedState.keySet());

                    if (migrationBuffer.notifyStateMerged(scalingContext,recordProcessorInScaling)){
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
    public <T> void processRecord(
            StreamRecord<T> record,
            InputChannelInfo channelInfo,
            int keyGroupIndex,
            Counter numRecordsIn,
            Input<T> input) throws Exception {
        // this relies on the fact that megaphone's fluid migration and micro-batch-alike migration:
        // that is, the migration buffer will always be cleared when the state is merged.
        // so when the buffer is not empty, we directly cache the record into the buffer
        // and do not need to try to process the records in the buffer.
        // A more general solution is to cache the record first and then try to process the records in the buffer


        if (channelInfo == null){
            checkState(scalingContext.isLocalKeyGroup(keyGroupIndex),
                    "Key group index " + keyGroupIndex + " is not local in task " + taskName);
            directProcess(record, numRecordsIn, input);
        }else if (!migrationBuffer.isEmpty() || scalingContext.isIncomingKey(keyGroupIndex)){
            if(!migrationBuffer.cacheRecord(record, channelInfo, keyGroupIndex)){
                LOG.info("{}: suspend the processing.", taskName);
                // if the buffer is full, then suspend the processing
                availabilityHelper.resetUnavailable();
            }
        }else {
            //if (scalingContext.isLocalKeyGroup(keyGroupIndex)){
            checkState(scalingContext.isLocalKeyGroup(keyGroupIndex),
                    "Key group index " + keyGroupIndex + " is not local in task " + taskName);
            directProcess(record, numRecordsIn, input);
        }
    }

    @Override
    public void onCompleteBarrier(SubtaskScaleResourceReleaser releaser) {
        LOG.info("{}: Received complete barrier, no more subsequent subscales will be triggered.", taskName);
        releaser.registerReleaseCallback(this::close);
        scalingContext.registerCompleteNotifier(releaser::notifyComplete);
    }

    @Override
    public void close() {
        pendingTriggerBarriers.clear();

    }
}
