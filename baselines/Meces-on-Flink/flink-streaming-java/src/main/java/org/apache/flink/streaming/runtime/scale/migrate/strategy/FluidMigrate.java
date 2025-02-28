package org.apache.flink.streaming.runtime.scale.migrate.strategy;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.coordinator.SubtaskScaleResourceReleaser;
import org.apache.flink.runtime.scale.io.ScaleCommListener;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.state.HierarchicalStateID;
import org.apache.flink.runtime.scale.state.KeyOrStateID;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.scale.migrate.MigrationBuffer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Fluid migration will split the whole migrated state into multiple small parts.
 * By which, we can migrate the state and concurrently process the rest of the state.
 */

public class FluidMigrate extends MigrateStrategy {

    protected final MigrationBuffer migrationBuffer;

    private final CompletableFuture<Void> stateMigrationReadyFuture = new CompletableFuture<>();
    protected final Map<Integer, CompletableFuture<Void>> stateTransmitCompletes = new ConcurrentHashMap<>();
    private final StateMigratingManager stateMigratingManager;

    private final Set<InputChannelInfo> blockedChannels = new HashSet<>();
    private final IndexedInputGate[] inputGates;

    private final Object suspendLock = new Object();
    HierarchicalStateID suspendBinID = null;
    CompletableFuture<ScaleBuffer.StateBuffer> suspendMainThreadFuture = null;
    Map<KeyOrStateID, AtomicBoolean> cancelFlags = new ConcurrentHashMap<>();
    Map<KeyOrStateID, ScaleBuffer.StateBuffer> mergingStateBufferCache = new ConcurrentHashMap<>();

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


        this.stateMigratingManager =
                new StateMigratingManager(
                        scalingContext,
                        scaleCommOutAdapter,
                        stateTransmitCompletes,
                        stateMigrationReadyFuture,
                        scaleMailConsumer,
                        mainOperator);

        this.migrationBuffer = new MigrationBuffer();
        this.inputGates = inputGates;
        scalingContext.setSuspendingCheckerAndBinHandler(availabilityHelper,stateMigratingManager::migrateInMainThread);

        LOG.info("Reset availability (current {})", availabilityHelper.isAvailable());
        availabilityHelper.resetAvailable();
    }

    @Override
    public void onTriggerBarrier(
            TriggerBarrier tb,
            InputChannelInfo channelInfo,
            SubtaskScaleResourceReleaser releaser) throws IOException {

        if (blockedChannels.isEmpty()){
            int subtaskIndex = scalingContext.getSubtaskIndex();
            Map<Integer, List<Integer>> sourceTaskWithInKeys = tb.getInKeys(subtaskIndex);
            Map<Integer, List<Integer>> targetTaskWithOutKeys = tb.getOutKeys(subtaskIndex);

            // main thread: prepare for receiving events and buffers
            scalingContext.initScale(sourceTaskWithInKeys, targetTaskWithOutKeys);

            // async request state migration in
            requestStatesAsync(sourceTaskWithInKeys);
            Set<Integer> allOutKeys = new HashSet<>();
            targetTaskWithOutKeys.values().forEach(allOutKeys::addAll);

            stateMigratingManager.startMigrateState(allOutKeys);

        }
        blockedChannels.add(channelInfo);

        LOG.info("{}: Received trigger barrier {}/{}.", taskName, blockedChannels.size(), targetChannelCount);
        if (blockedChannels.size() == targetChannelCount){
            scalingContext.markAligned();
            for (InputChannelInfo blockedChannel : blockedChannels){
                inputGates[blockedChannel.getGateIdx()].resumeConsumption(blockedChannel);
            }
            Set<Integer> allTargetTasks = scalingContext.getAllTargetTasks();

            releaser.registerReleaseCallback(this::close);
            scalingContext.registerCompleteNotifier(releaser::notifyComplete);

            CompletableFuture.runAsync(
                    ()->{
//                        for (Integer allTargetTask : allTargetTasks) {
//                            ScaleEvent alignedAck = new ScaleEvent.AlignedAck(subtaskIndex);
//                            try {
//                                LOG.info("{}: Sending aligned ack to subtask {}.", taskName, allTargetTask);
//                                scaleCommOutAdapter.sendEvent(allTargetTask, alignedAck);
//                            } catch (Exception e) {
//                                LOG.error("Failed to send aligned ack to subtask {}.", allTargetTask, e);
//                                throw new RuntimeException(e);
//                            }
//                        }
                        scaleCommOutAdapter.notifyAlign(allTargetTasks);
                    }
            );
        }

    }

    // ------------------ state migration ------------------
    private CompletableFuture<Void> requestStatesAsync(Map<Integer, List<Integer>> sourceTaskWithInKeys) {
        // async request states
        return CompletableFuture.runAsync(() -> {
            sourceTaskWithInKeys.forEach((sourceSubtask, inKeys) -> {
                ScaleEvent requestStateEvent = new ScaleEvent.StatesRequest(subtaskIndex, inKeys);
                try {
                    LOG.info("{}: Sending request state event to subtask {}.", taskName, sourceSubtask);
                    //scaleCommOutAdapter.sendEvent(sourceSubtask, requestStateEvent);
                    scaleCommOutAdapter.requestStatesWithCreatingChannel(sourceSubtask, requestStateEvent);
                } catch (Exception e) {
                    LOG.error("Failed to send request state event to subtask {}.", sourceSubtask, e);
                    throw new RuntimeException(e);
                }
            });
        });
    }

    @Override
    public void processEvent(ScaleEvent event, Channel channel){
        if (event instanceof ScaleEvent.StatesRequest) {
            final ScaleEvent.StatesRequest statesRequest = (ScaleEvent.StatesRequest) event;
            scaleCommOutAdapter.onRequestedStatesReceived(event.eventSenderIndex, channel);
            stateMigratingManager.notifyStatesMigratedReady(statesRequest.requestedStates);

        } else if (event instanceof ScaleEvent.FetchRequest) {
            HierarchicalStateID fetchedStateID = ((ScaleEvent.FetchRequest) event).fetchedStateID;
            try {
                handleFetchRequest(fetchedStateID, event.eventSenderIndex);
            } catch (Exception e) {
                LOG.error("Failed to handle fetch request.", e);
                throw new RuntimeException(e);
            }
        }  else if (event instanceof ScaleEvent.Acknowledge){
            ScaleEvent.Acknowledge ack = (ScaleEvent.Acknowledge) event;
            int stillTransferring =  scalingContext.transferringCount.decrementAndGet();
            LOG.info("Received acknowledge for {} and {} still transferring.", ack.ackedID, stillTransferring);
            if (ack.ackedID.isKey()){
                CompletableFuture<Void> transmitComplete = stateTransmitCompletes.remove(ack.ackedID.getKey());
                transmitComplete.complete(null);
            }
            if (stillTransferring == 0){
                scaleMailConsumer.accept(
                        scalingContext::tryCompleteScale,
                        "Notify state migration completed"
                );
            }
        } else if (event instanceof ScaleEvent.NoBinAck){
            ScaleEvent.NoBinAck noBinAck = (ScaleEvent.NoBinAck) event;
            LOG.info("Received no bin ack for suspended bin {}.", noBinAck.noBinAckID);

            BiConsumer<Long, HierarchicalStateID> reFetch = (sleepTimeMs, binID) -> {
                LOG.info("try re-fetching {} due to still suspended.", noBinAck.noBinAckID);
                // wait a while and retry (in case of too many requests)
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                Thread.sleep(sleepTimeMs);
                            } catch (InterruptedException e) {
                                LOG.error("Failed to sleep.", e);
                                throw new RuntimeException(e);
                            }
                        }
                ).thenRunAsync(
                        () -> stateMigratingManager.fetch(binID, event.eventSenderIndex)
                );
            };

            synchronized (suspendLock){
                if (suspendBinID != null && suspendBinID.equals(noBinAck.noBinAckID)){
                    reFetch.accept(200L, suspendBinID);
                    return;
                }
            }

            scaleMailConsumer.accept(
                    () -> {
                        if ( migrationBuffer.getCachedBinID() != null && migrationBuffer.getCachedBinID().equals(noBinAck.noBinAckID)){
                            reFetch.accept(400L, migrationBuffer.getCachedBinID());
                        }
                    },
                    "handle fetch request"
            );
        } else if ( event instanceof ScaleEvent.AlignedAck) {
            LOG.info("Received aligned ack from subtask {}.", event.eventSenderIndex);
            scaleMailConsumer.accept(
                    () -> scalingContext.markSourceAligned(event.eventSenderIndex),
                    "mark source aligned"
            );
        } else {
            throw new IllegalArgumentException("Unknown event type: " + event);
        }
    }

    // to avoid deadlock, we need to handle fetch request while main thread is suspended
    private void handleFetchRequest(HierarchicalStateID fetchedStateID, int fromTaskIndex) throws Exception {

        synchronized (suspendLock){
            if (suspendBinID != null){
                LOG.info("Processing fetch request {} while main thread is suspended.", fetchedStateID);

                // try to merge all cached state buffers in this thread first
                for (Map.Entry<KeyOrStateID, ScaleBuffer.StateBuffer> entry : mergingStateBufferCache.entrySet()) {
                    KeyOrStateID cachedKeyOrStateID = entry.getKey();
                    ScaleBuffer.StateBuffer stateBuffer = entry.getValue();
                    LOG.info("merge {} in Thread {}",
                            stateBuffer,
                            Thread.currentThread().getName());
                    BitSet binFlags = mainOperator.mergeState(stateBuffer);
                    scalingContext.onMerge(cachedKeyOrStateID, binFlags);
                    if (migrationBuffer.notifyStateOrBinMerged(
                            scalingContext,
                            recordProcessorInScaling)){
                        LOG.info("Resume suspend due to {} merged", cachedKeyOrStateID);
                        CompletableFuture toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        toNotify.complete(null);
                        scalingContext.tryCompleteScale();
                    }
                    cancelFlags.remove(cachedKeyOrStateID).set(true);
                }
                mergingStateBufferCache.clear();


                if(scalingContext.isLocalBin(fetchedStateID)){
                    // since main thread is not available,it is safe to modify object directly
                    stateMigratingManager.migrateInMainThread(
                            fromTaskIndex,
                            new KeyOrStateID(fetchedStateID));
                }else{
                    try {
                        scaleCommOutAdapter.sendEvent(
                                fromTaskIndex,
                                new ScaleEvent.NoBinAck(subtaskIndex, fetchedStateID));
                    } catch (Exception e) {
                        LOG.error(
                                "Failed to send state buffer to subtask {}.",
                                fromTaskIndex,
                                e);
                        throw new RuntimeException(e);
                    }
                }
                return;
            }
        }
        scaleMailConsumer.accept(
                () -> handleFetchRequestInternal(fetchedStateID, fromTaskIndex),
                "handle fetch request"
        );
    }

    private void handleFetchRequestInternal(HierarchicalStateID fetchedStateID, int fromTaskIndex) {
        if (scalingContext.isLocalBin(fetchedStateID)) {
            LOG.info("Processing fetch request for local bin {}", fetchedStateID);

            stateMigratingManager.migrateInMainThread(
                    fromTaskIndex,
                    new KeyOrStateID(fetchedStateID));

        }else{
            LOG.info("Received fetch request for non-local bin {}", fetchedStateID);
            CompletableFuture.runAsync(
                    () -> {
                        try {
                            scaleCommOutAdapter.sendEvent(
                                    fromTaskIndex,
                                    new ScaleEvent.NoBinAck(subtaskIndex, fetchedStateID));
                        } catch (Exception e) {
                            LOG.error(
                                    "Failed to send state buffer to subtask {}.",
                                    fromTaskIndex,
                                    e);
                            throw new RuntimeException(e);
                        }
                    }
            );
        }
    }

    // not in main thread
    @Override
    public void processBuffer(ScaleBuffer buffer, int fromTaskIndex){
        if (buffer instanceof ScaleBuffer.StateBuffer){
            ScaleBuffer.StateBuffer stateBuffer = (ScaleBuffer.StateBuffer) buffer;
            Set<KeyOrStateID> keyOrStateIDs = stateBuffer.outgoingManagedKeyedState.keySet();
            KeyOrStateID keyOrStateID = keyOrStateIDs.iterator().next();
            LOG.info("Received {} from subtask {}",
                    keyOrStateID, fromTaskIndex);
            checkNotNull(keyOrStateID, "Key or state ID is null.");
            sendAckAsync(fromTaskIndex, keyOrStateID);

            synchronized (suspendLock){
                if (suspendBinID != null){
                    checkNotNull(suspendMainThreadFuture, "Suspend main thread future is null.");
                    if (!keyOrStateID.isKey() && keyOrStateID.getStateID().equals(suspendBinID)){
                        LOG.info("Received state buffer for suspended bin {}.", suspendBinID);
                        suspendMainThreadFuture.complete(stateBuffer);
                        return;
                    }else if (keyOrStateID.isKey() && keyOrStateID.getKey() == suspendBinID.keyGroupIndex && stateBuffer.binFlags.get(suspendBinID.binIndex)){
                        LOG.info("Received key buffer for suspended bin {}.", suspendBinID);
                        suspendMainThreadFuture.complete(stateBuffer);
                        return;
                    }
                }
                cancelFlags.put(keyOrStateID, new AtomicBoolean(false));
                mergingStateBufferCache.put(keyOrStateID, stateBuffer);
            }

            stateMigratingManager.mergeStateOrBin(
                    stateBuffer,
                    migrationBuffer,
                    mainOperator,
                    recordProcessorInScaling,
                    availabilityHelper,
                    cancelFlags.get(keyOrStateID),
                    ()-> {
                        synchronized (suspendLock){
                            cancelFlags.remove(keyOrStateID);
                            mergingStateBufferCache.remove(keyOrStateID);
                        }
                    });
        } else{
            throw new IllegalArgumentException("Unknown buffer type: " + buffer);
        }
    }

    private void sendAckAsync(int fromTaskIndex, KeyOrStateID keyOrStateID) {
        scalingContext.sendingAckCount.incrementAndGet();
        CompletableFuture.runAsync(() -> {
                try {
                    LOG.info("Sending acknowledge {} to subtask {}.", keyOrStateID, fromTaskIndex);
                    scaleCommOutAdapter.sendEvent(
                            fromTaskIndex,
                            new ScaleEvent.Acknowledge(subtaskIndex, keyOrStateID));
                    if (scalingContext.sendingAckCount.decrementAndGet() == 0){
                        scaleMailConsumer.accept(
                                scalingContext::tryCompleteScale,
                                "Notify state migration completed"
                        );
                    }
                } catch (InterruptedException e) {
                    LOG.error("Failed to send acknowledge state event.", e);
                    throw new RuntimeException(e);
                }
        });
    }

    @Override
    public <T> void processRecord(
            StreamRecord<T> record,
            InputChannelInfo channelInfo,
            int keyGroupIndex,
            int subBinIndex,
            Counter numRecordsIn,
            Input<T> input) throws Exception {

        HierarchicalStateID binID = new HierarchicalStateID(keyGroupIndex, subBinIndex);

        if (scalingContext.isLocalBin(binID)){
            // 0. local keys
            try {
                directProcess(record, numRecordsIn, input);
            }catch (Exception e){
                LOG.error("Failed to process {}.", binID, e);
                scalingContext.logInfoDuringError();
                throw e;
            }
        }else{
            int remoteTask = scalingContext.getRelatedTask(binID.keyGroupIndex);
            migrationBuffer.cacheRecord(record, channelInfo, binID);
            availabilityHelper.resetUnavailable();
            stateMigratingManager.fetch(binID, remoteTask);
        }

//        if (channelInfo == null){
//            directProcess(record, numRecordsIn, input);
//            return;
//        }
//        HierarchicalStateID binID = new HierarchicalStateID(keyGroupIndex, subBinIndex);
//
//        if(migrationBuffer.isEmpty()){
//            if (scalingContext.isLocalBin(binID)){
//                // 0. local keys
//                try {
//                    directProcess(record, numRecordsIn, input);
//                }catch (Exception e){
//                    LOG.error("Failed to process {}.", binID, e);
//                    scalingContext.logInfoDuringError();
//                    throw e;
//                }
//            }else{
//                int remoteTask = scalingContext.getRelatedTask(binID.keyGroupIndex);
//                migrationBuffer.cacheRecord(record, channelInfo, binID);
//                //availabilityHelper.resetUnavailable();
//                stateMigratingManager.fetch(binID, remoteTask);
//            }
//        }else{
//            migrationBuffer.cacheRecord(record, channelInfo, binID);
//            // 1. process cached records from
//            while(!migrationBuffer.isEmpty()){
//                Tuple2<StreamRecord, HierarchicalStateID> cachedRecord = migrationBuffer.peek();
//                if(scalingContext.isLocalBin(cachedRecord.f1)) {
//                    try {
//                        directProcess(cachedRecord.f0, numRecordsIn, input);
//                    } catch (Exception e) {
//                        LOG.error("Failed to process {}.", cachedRecord.f1, e);
//                        scalingContext.logInfoDuringError();
//                        throw e;
//                    }
//                    migrationBuffer.poll();
//                }else{
//                    break;
//                }
//            }
//            if (!migrationBuffer.isEmpty()){
//                if (!migrationBuffer.fetched){
//                    Tuple2<StreamRecord, HierarchicalStateID> cachedRecord = migrationBuffer.peek();
//                    int remoteTask = scalingContext.getRelatedTask(cachedRecord.f1.keyGroupIndex);
//                    stateMigratingManager.fetch(cachedRecord.f1, remoteTask);
//                    migrationBuffer.fetched = true;
//                }
//
//                if (migrationBuffer.isFull()){
//                    availabilityHelper.resetUnavailable();
//                }
//            }
//        }

    }

    @Override
    public boolean trySetCurrentKey(Object key) throws Exception {
        if(key == null){
            scalingContext.logInfoDuringError();
            return false;
        }

        HierarchicalStateID binID = mainOperator.getHieraStateIDByKey(key);
        //LOG.info("Try set current {}.", binID);
        if (scalingContext.isMigratedOutKey(binID.keyGroupIndex)){
            return false;
        }

        if (scalingContext.isLocalBin(binID)) {
            return true;
        }

        KeyOrStateID keyOrStateID = new KeyOrStateID(binID);
        LOG.info("Try set currentKey {}.", keyOrStateID);
        LOG.info("mainTread is not available at {}.", System.currentTimeMillis());

        synchronized (suspendLock) {
            // first try to merge all cached state buffers in this main Thread
            for (Map.Entry<KeyOrStateID, ScaleBuffer.StateBuffer> entry : mergingStateBufferCache.entrySet()) {
                KeyOrStateID cachedKeyOrStateID = entry.getKey();
                ScaleBuffer.StateBuffer stateBuffer = entry.getValue();
                LOG.info("merge {} in Thread {}",
                        stateBuffer,
                        Thread.currentThread().getName());
                BitSet binFlags = mainOperator.mergeState(stateBuffer);
                scalingContext.onMerge(cachedKeyOrStateID, binFlags);
                if (migrationBuffer.notifyStateOrBinMerged(
                        scalingContext,
                        recordProcessorInScaling)){
                    LOG.info("Resume suspend due to {} merged", cachedKeyOrStateID);
                    CompletableFuture toNotify = availabilityHelper.getUnavailableToResetAvailable();
                    toNotify.complete(null);
                    scalingContext.tryCompleteScale();
                }
                cancelFlags.remove(cachedKeyOrStateID).set(true);
            }
            mergingStateBufferCache.clear();

            if (scalingContext.isLocalBin(binID)){
                // those waiting for merging involves current needed bin
                LOG.info("mainTread is available at {}.", System.currentTimeMillis());
                return true;
            }
            this.suspendMainThreadFuture = new CompletableFuture<>();
            this.suspendBinID = binID;
        }

        int remoteTask = scalingContext.getRelatedTask(binID.keyGroupIndex);

        LOG.info("Thread {} not available, need to suspend and fetch {} from {}.", Thread.currentThread().getName(),binID, remoteTask);
        stateMigratingManager.fetch(binID, remoteTask);

        ScaleBuffer.StateBuffer stateBuffer = this.suspendMainThreadFuture.join(); // block main thread until the bin is fetched
        synchronized (suspendLock){
            this.suspendBinID = null;
            this.suspendMainThreadFuture = null;
        }
        KeyOrStateID mergedKeyOrStateID = (KeyOrStateID) stateBuffer.outgoingManagedKeyedState.keySet().iterator().next();
        // directly merge the state buffer
        LOG.info("merge {} in Thread {}",
                stateBuffer,
                Thread.currentThread().getName());
        BitSet binFlags = mainOperator.mergeState(stateBuffer);
        scalingContext.onMerge(mergedKeyOrStateID, binFlags);
        if (migrationBuffer.notifyStateOrBinMerged(
                    scalingContext,
                 recordProcessorInScaling)){
            LOG.info("Resume suspend due to {} merged", mergedKeyOrStateID);
            CompletableFuture toNotify = availabilityHelper.getUnavailableToResetAvailable();
            toNotify.complete(null);
            scalingContext.tryCompleteScale();
        }
        LOG.info("mainTread is available at {}.", System.currentTimeMillis());
        if (!scalingContext.isLocalBin(binID)){
            scalingContext.logInfoDuringError();
            throw new IllegalStateException("Bin " + binID + " is still not local.");
        }
        return true;
    }

    @Override
    public void close() {
        super.close();
    }

}
