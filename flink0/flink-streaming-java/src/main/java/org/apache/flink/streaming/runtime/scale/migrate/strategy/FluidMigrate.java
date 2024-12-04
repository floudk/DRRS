package org.apache.flink.streaming.runtime.scale.migrate.strategy;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.coordinator.SubtaskScaleResourceReleaser;
import org.apache.flink.runtime.scale.io.ScaleCommListener;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.io.message.RerouteCache;
import org.apache.flink.runtime.scale.io.message.local.StateBuffer;
import org.apache.flink.runtime.scale.io.message.barrier.ConfirmBarrier;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.scale.migrate.MigrationBuffer;
import org.apache.flink.streaming.runtime.scale.migrate.strategy.reroute.LocalRerouteCache;
import org.apache.flink.streaming.runtime.scale.migrate.strategy.reroute.RemoteRerouteCache;
import org.apache.flink.streaming.runtime.scale.migrate.strategy.reroute.RerouteManager;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * Fluid migration will split the whole migrated state into multiple small parts.
 * By which, we can migrate the state and concurrently process the rest of the state.
 */

public class FluidMigrate extends MigrateStrategy {

    protected final MigrationBuffer migrationBuffer;

    private final Map<Integer, RerouteManager>  rerouteManagers = new ConcurrentHashMap<>();
    protected final Map<Integer, CompletableFuture<Void>> stateTransmitCompletes = new ConcurrentHashMap<>();

    protected final Set<TriggerBarrier> pendingTriggerBarriers = new HashSet<>();
    protected final Map<ConfirmBarrier, Integer> reroutedConfirmBarriers = new HashMap<>();

    private final ConcurrentHashMap<RerouteID, RerouteProcessingTracker>
            rerouteProcessingTrackers = new ConcurrentHashMap<>();


    public FluidMigrate(
            StreamOperator mainOperator,
            ScaleCommOutAdapter scaleCommOutAdapter,
            ScaleCommListener scaleCommListener,
            ScalingContext scalingContext,
            IndexedInputGate[] inputGates,
            BiConsumer<RunnableWithException,String> poisonMailbox,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling) {
        super(
                mainOperator,
                scaleCommOutAdapter,
                scaleCommListener,
                scalingContext,
                Arrays.stream(inputGates).map(IndexedInputGate::getNumberOfInputChannels).mapToInt(Integer::intValue).sum(),
                poisonMailbox,
                recordProcessorInScaling);

        this.migrationBuffer = new MigrationBuffer(inputGates);

        availabilityHelper.resetAvailable(); // reset the availability in the beginning
    }

    @Override
    public void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo) throws IOException {
        if (pendingTriggerBarriers.contains(tb)) {
            // only react to the first barrier
            return;
        }
        LOG.info("Received new trigger barrier from channel {}.",  channelInfo);
        pendingTriggerBarriers.add(tb);
        int subtaskIndex = scalingContext.getSubtaskIndex();
        Map<Integer, List<Integer>> sourceTaskWithInKeys = tb.getInKeys(subtaskIndex);
        Map<Integer, List<Integer>> targetTaskWithOutKeys = tb.getOutKeys(subtaskIndex);

        // main thread: prepare for receiving events and buffers
        scalingContext.subscale(sourceTaskWithInKeys, targetTaskWithOutKeys, tb.subscaleID);

        migrationBuffer.subscale(sourceTaskWithInKeys);

        // async request state migration in
        requestStatesAsync(sourceTaskWithInKeys,tb.subscaleID);
    }

    @Override
    public void onConfirmBarrier(ConfirmBarrier cb, InputChannelInfo channelInfo) {

        Set<Integer> targetTasks = scalingContext.getOutInvolvedTasks(cb.getInvolvedKeys());
        if (targetTasks.isEmpty()){
            // ignore the unrelated confirm barrier
            return;
        }
        reroutedConfirmBarriers.compute(cb, (k, v) -> v == null ? 1 : v + 1);

        CompletableFuture<Void> confirmBarrierConsumedFuture = new CompletableFuture<>();
        int subscaleID = cb.subscaleID;

        // create if not exist
        rerouteManagers.computeIfAbsent(
                subscaleID,
                (k) -> new RerouteManager(scalingContext, scaleCommOutAdapter, subscaleID))
                .rerouteConfirmBarrier(cb, targetTasks, channelInfo, confirmBarrierConsumedFuture);

        LOG.info("Reroute confirm barrier {} ({}) to target tasks {}: {}/{}",
                cb, cb.getInvolvedKeys(), targetTasks, reroutedConfirmBarriers.get(cb), targetChannelCount);

        if(reroutedConfirmBarriers.get(cb) == targetChannelCount){
            confirmBarrierConsumedFuture.thenRun(
                    () -> {
                        scaleMailConsumer.accept(
                                () -> {
                                    // these states are all migrated out completely
                                    LOG.info(
                                            "{}: Completely migrated out states in {}.",
                                            taskName,
                                            cb.getInvolvedKeys());
                                    scalingContext.notifyAllConfirmBarriersRerouted(cb.getInvolvedKeys());
                                },
                                "Notify all confirm barriers rerouted");
                        rerouteManagers.get(subscaleID).noMoreBarriersFuture.complete(null);
                    }
            );
        }
    }


    // ------------------ state migration ------------------
    protected CompletableFuture<Void> requestStatesAsync(Map<Integer, List<Integer>> sourceTaskWithInKeys, int subscaleID) {
        // async request states
        return CompletableFuture.runAsync(() -> {
            sourceTaskWithInKeys.forEach((sourceSubtask, inKeys) -> {
                try {
                    LOG.info("{}: Sending request state event to subtask {}.", taskName, sourceSubtask);
                    scaleCommOutAdapter.requestStatesWithCreatingChannel(
                            sourceSubtask,
                            inKeys,
                            subscaleID);
                } catch (Exception e) {
                    LOG.error("Failed to send request state event to subtask {}.", sourceSubtask, e);
                    throw new RuntimeException(e);
                }
            });
        });
    }

    @Override
    public void processEvent(ScaleEvent event, Channel channel){
        if (event instanceof ScaleEvent.RequestStates) {
            LOG.info("{}: Received request state transfer event {}.", taskName, event);
            final ScaleEvent.RequestStates requestStates = (ScaleEvent.RequestStates) event;
            scaleCommOutAdapter.onRequestedStatesReceived(requestStates.subscaleID, requestStates.eventSenderIndex,channel);
            // start migrating states
            migrateStatesAsync(requestStates.eventSenderIndex, requestStates.requestedStates,requestStates.subscaleID);
        }else if (event instanceof ScaleEvent.AcknowledgeStates) {
            // notify the state has been transmitted
            final int keyGroupIndex = ((ScaleEvent.AcknowledgeStates) event).keyGroupIndex;
            LOG.info("{}: Received acknowledge state transfer event {}.", taskName, keyGroupIndex);
            scaleMailConsumer.accept(
                    () -> {
                        scalingContext.removeFromOutPendingKeys(List.of(keyGroupIndex));
                    },
                    "Notify state transmitted"
            );
            CompletableFuture<Void> transmitComplete = stateTransmitCompletes.remove(keyGroupIndex);
            checkNotNull(transmitComplete, "KeyGroupIndex " + keyGroupIndex + " is not found.");
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
            int keyGroupIndex = (int) stateBuffer.outgoingManagedKeyedState.keySet().iterator().next();
            CompletableFuture.runAsync(
                    () -> {
                        LOG.info("{}: Sending acknowledge {} to subtask {}.",
                                taskName,
                                keyGroupIndex,
                                fromTaskIndex);
                        scaleCommOutAdapter.ackStateReceived(fromTaskIndex, keyGroupIndex, scalingContext.getSubscaleID(keyGroupIndex));
                    });
            mergeState(stateBuffer, fromTaskIndex);
        }else if(buffer instanceof ScaleBuffer.RecordBuffer){
            checkArgument(ScaleConfig.Instance.ENABLE_DR, "Reroute record buffer is only supported in DR mode.");
            final ScaleBuffer.RecordBuffer recordBuffer = (ScaleBuffer.RecordBuffer) buffer;
            // process rerouted record and potential confirm barrier
            RerouteCache cache = recordBuffer.rerouteCache;
            LOG.info("process rerouted record buffer {}-{} from {}",
                    "hasCache=" + (cache != null),
                    "hasConfirmBarrier=" + (recordBuffer.withConfirmBarrier ? recordBuffer.confirmedKeys : null), fromTaskIndex);

            Queue<StreamRecord> records = null;
            if (cache != null){
                if (cache instanceof RerouteCache.RemoteDelegate){
                    final ByteBuf delegate = ((RerouteCache.RemoteDelegate) cache).delegate;
                    try {
                        LOG.info("{}: deserialize remote reroute cache ({} bytes) in Thread {}",
                                taskName,
                                delegate.readableBytes(),
                                Thread.currentThread().getName());
                        records = RemoteRerouteCache.deserialize(
                                delegate,
                                scaleCommOutAdapter.getInputSerializer().duplicate());
                    } catch (IOException e) {
                        LOG.error("Failed to deserialize remote reroute cache.", e);
                        throw new RuntimeException(e);
                    }
                }else if (cache instanceof LocalRerouteCache) {
                    records = ((LocalRerouteCache) cache).buffer;
                }else{
                    throw new IllegalArgumentException("Unknown reroute cache type: " + cache);
                }
            }
            
            final Queue<StreamRecord> finalRecords = records;
            RunnableWithException toMainThreadTask = () -> {
                if (finalRecords != null){
                    LOG.info("{}: process {} rerouted records in Thread {}",
                            taskName,
                            finalRecords.size(),
                            Thread.currentThread().getName());
                    finalRecords.forEach((record) -> {
                                try {
                                    recordProcessorInScaling.accept(record, null);
                                } catch (Exception e) {
                                    LOG.error("Failed to process rerouted record.", e);
                                    throw new RuntimeException(e);
                                }
                            }
                    );
                }
                if (recordBuffer.withConfirmBarrier){
                    checkNotNull(recordBuffer.inputChannelInfo, "Input channel info is not set yet in task " + taskName);
                    checkNotNull(recordBuffer.confirmedKeys, "Confirmed keys are not set yet in task " + taskName);


                    Set<Integer> confirmedKeys = getConfirmedKeys(recordBuffer.confirmedKeys, fromTaskIndex);
                    LOG.info("{}: process rerouted confirm barrier {}-{} in Thread {}",
                            taskName,
                            recordBuffer.inputChannelInfo,
                            recordBuffer.confirmedKeys,
                            Thread.currentThread().getName());
                    if (migrationBuffer.notifyRemoteConfirmed(
                            recordBuffer.inputChannelInfo,
                            confirmedKeys,
                            scalingContext,
                            recordProcessorInScaling,
                            fromTaskIndex)){
                        LOG.info("{}: remote confirm barrier received, reset availability", taskName);
                        CompletableFuture toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        toNotify.complete(null);
                    }
                }
            };

            RerouteID rerouteID =
                    new RerouteID(fromTaskIndex, recordBuffer.subscaleID);
            int sequenceNumber = recordBuffer.sequenceNumber;
            RerouteProcessingTracker tracker = rerouteProcessingTrackers.computeIfAbsent(
                    rerouteID, (k) -> new RerouteProcessingTracker());

            synchronized (tracker){
                if (sequenceNumber == tracker.expectedSequenceNumber){
                    scaleMailConsumer.accept(
                            toMainThreadTask,
                            "Process rerouted records and confirm barrier"
                    );
                    tracker.expectedSequenceNumber++;

                    while (tracker.pendingRerouteRecords.containsKey(tracker.expectedSequenceNumber)){
                        scaleMailConsumer.accept(
                                tracker.pendingRerouteRecords.remove(tracker.expectedSequenceNumber),
                                "Process rerouted records and confirm barrier"
                        );
                        tracker.expectedSequenceNumber++;
                    }
                }else if (sequenceNumber > tracker.expectedSequenceNumber){
                    LOG.info("{} Expected sequence number: {}, received: {}, buffer is buffered.",
                            rerouteID.subscaleID + "-" + rerouteID.fromTaskIndex,
                            tracker.expectedSequenceNumber,
                            sequenceNumber);
                    tracker.pendingRerouteRecords.put(sequenceNumber, toMainThreadTask);
                }else{
                    LOG.error("Received out-of-order rerouted record buffer: {}, expected: {}",
                            sequenceNumber, tracker.expectedSequenceNumber);
                }
            }
        }else{
            throw new IllegalArgumentException("Unknown buffer type: " + buffer);
        }
    }

    protected Set<Integer> getConfirmedKeys(Set<Integer> confirmedKeys, int sourceSubtask){
        return confirmedKeys;
    }

    protected void migrateStatesAsync(int targetTaskIndex,List<Integer> requestedStates,int subscaleID){
        CompletableFuture.runAsync(() -> {
            // create if not exist
            rerouteManagers.computeIfAbsent(
                    subscaleID,
                    (k) -> new RerouteManager(scalingContext, scaleCommOutAdapter, subscaleID));

            requestedStates.forEach(
                    (keygroup)->{
                        try {
                            CompletableFuture<Void> transmitComplete = new CompletableFuture<>();
                            stateTransmitCompletes.put(keygroup, transmitComplete);
                            final List<Integer> keygroups = List.of(keygroup);
                            CompletableFuture<Void> adapterConsumeFuture = new CompletableFuture<>();
                            collectAndMigrateStates(
                                    targetTaskIndex, keygroups, adapterConsumeFuture, subscaleID);
                            rerouteManagers.get(subscaleID).setStateConsumeFuture(keygroup, adapterConsumeFuture);
                            // wait for the state to be transmitted
                            transmitComplete.join();
                        } catch (Exception e) {
                            LOG.error("Failed to migrate state for keygroup {}.", keygroup, e);
                            throw new RuntimeException(e);
                        }
                    }
            );
            // close rerouteManagerAfterAligned
        });
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

                    if (migrationBuffer.notifyStateMerged(
                            stateBuffer.outgoingManagedKeyedState.keySet(),recordProcessorInScaling)){
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

         // String keyWithCount = record.getValue().toString();

        if (channelInfo == null){
            checkArgument(ScaleConfig.Instance.ENABLE_DR, "Reroute record buffer is only supported in DR mode.");
            // these records are from source task(re-routed records)
//             LOG.info("{}: in scaling process record: {}, rerouted record--direct process",
//                     taskName,
//                     keyWithCount);
            directProcess(record, numRecordsIn, input);
            return;
        }

        if (scalingContext.isIncomingKey(keyGroupIndex)){
            // the keys that are still in migrating-in process,
            // that is, do not meet both the following 2 demands
            // 1. state has been transmitted
            // 2. all remote channel confirmed
            if (migrationBuffer.isRemoteConfirmed(channelInfo,keyGroupIndex)
                    && scalingContext.isLocalKeyGroup(keyGroupIndex)){
//                 LOG.info("{}: in scaling process record: {}, remote confirmed {} and state {} transmitted--direct process",
//                         taskName,
//                         keyWithCount,
//                         channelInfo,
//                         keyGroupIndex);
                directProcess(record, numRecordsIn, input);
            }else{
//                 LOG.info("{}: in scaling process record: {}, not confirmed {} or state {} not transmitted--cache record",
//                         taskName,
//                         record.getValue().toString(),
//                         channelInfo,
//                         keyGroupIndex);
                // try to add to migration buffer, if the buffer is full, then suspend the processing
                if (!migrationBuffer.cacheRecord(record, channelInfo, keyGroupIndex)){
                    LOG.info("{}: Full migration buffer, suspend processing at {}.", taskName, System.currentTimeMillis());
                    // if the buffer is full, then suspend the processing
                    availabilityHelper.resetUnavailable();
                }
            }
        }else if (scalingContext.isLocalKeyGroup(keyGroupIndex)){
//             LOG.info("{}: in scaling process record: {}-{}, local key group--direct process",
//                     taskName,
//                     keyGroupIndex,
//                     keyWithCount);
            directProcess(record, numRecordsIn, input);
        }else{
//             LOG.info("{}: in scaling process record: {}, migrating or migrated out--reroute record",
//                     taskName,
//                     keyWithCount);
            // re-route the record to the corresponding task
            int subscaleID = scalingContext.getSubscaleID(keyGroupIndex);
            reroute(record, keyGroupIndex, subscaleID);
        }
    }

    protected void reroute(StreamRecord record, int keyGroupIndex, int subscaleID){
        rerouteManagers.get(subscaleID).rerouteRecord(record, keyGroupIndex);
    }

    @Override
    public void onCompleteBarrier(SubtaskScaleResourceReleaser releaser) {
        LOG.info("{}: Received complete barrier, no more subsequent subscales will be triggered.", taskName);
        releaser.registerReleaseCallback(this::close);
        scalingContext.registerCompleteNotifier(releaser::notifyComplete);
    }

    @Override
    public void close() {

    }
    private static class RerouteProcessingTracker {
        int expectedSequenceNumber;
        Map<Integer, RunnableWithException> pendingRerouteRecords = new TreeMap<>();
    }

    static class RerouteID {
        public final int fromTaskIndex;
        public final int subscaleID;
        public RerouteID(int fromTaskIndex, int subscaleID) {
            this.fromTaskIndex = fromTaskIndex;
            this.subscaleID = subscaleID;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RerouteID)) return false;
            RerouteID rerouteID = (RerouteID) o;
            return fromTaskIndex == rerouteID.fromTaskIndex && subscaleID == rerouteID.subscaleID;
        }
        @Override
        public int hashCode() {
            return 31 * fromTaskIndex + subscaleID;
        }
    }

}
