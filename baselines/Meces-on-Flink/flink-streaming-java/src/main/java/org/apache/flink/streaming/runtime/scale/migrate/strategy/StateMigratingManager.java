package org.apache.flink.streaming.runtime.scale.migrate.strategy;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.scale.ScalingContext;

import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.state.HierarchicalStateID;
import org.apache.flink.runtime.scale.state.KeyOrStateID;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.scale.migrate.MigrationBuffer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class StateMigratingManager {

    private static final Logger LOG = LoggerFactory.getLogger(StateMigratingManager.class);

    final ScalingContext scalingContext;
    final ScaleCommOutAdapter scaleCommOutAdapter;

    final CompletableFuture<Void> stateMigrationReadyFuture;
    final Map<Integer, CompletableFuture<Void>> stateTransmitCompletes; // to fluid migrateStateAsync

    // collecting should be executed in main thread
    final BiConsumer<RunnableWithException,String> scaleMailConsumer;
    final StreamOperator mainOperator;

    final Object availableLock = new Object();
    final Set<Integer> available = new HashSet<>();
    Set<Integer> allToMigrate;

    public StateMigratingManager(
            ScalingContext scalingContext,
            ScaleCommOutAdapter scaleCommOutAdapter,
            Map<Integer, CompletableFuture<Void>> stateTransmitCompletes,
            CompletableFuture<Void> stateMigrationReadyFuture,
            BiConsumer<RunnableWithException,String> scaleMailConsumer,
            StreamOperator mainOperator) {

        this.scalingContext = scalingContext;
        this.scaleCommOutAdapter = scaleCommOutAdapter;

        this.stateTransmitCompletes = stateTransmitCompletes;
        this.stateMigrationReadyFuture = stateMigrationReadyFuture;

        this.scaleMailConsumer = scaleMailConsumer;
        this.mainOperator = mainOperator;
    }

    public void startMigrateState(Set<Integer> allToMigrate){
        this.allToMigrate = allToMigrate;
        CompletableFuture.runAsync(this::fluidMigrate);
    }

    private void fluidMigrate(){
        LOG.info("Start fluid migrate states {} to task {} ",
                allToMigrate, scalingContext.getSubtaskIndex());
        while (!allToMigrate.isEmpty()){
            int keyGroup;
            synchronized (availableLock){
                while (available.isEmpty()){
                    try {
                        availableLock.wait();
                    } catch (InterruptedException e) {
                        LOG.error("Failed to wait for available keygroup.", e);
                        throw new RuntimeException(e);
                    }
                }
                keyGroup = available.iterator().next();
                available.remove(keyGroup);
            }
            CompletableFuture<Void> transmitComplete = new CompletableFuture<>();
            stateTransmitCompletes.put(keyGroup, transmitComplete);
            KeyOrStateID toMigrate = new KeyOrStateID(keyGroup);
            int finalKeyGroup = keyGroup;
            scaleMailConsumer.accept(
                    ()-> {
                        int targetTaskIndex = scalingContext.getRelatedTask(finalKeyGroup);
                        migrateInMainThread(targetTaskIndex, toMigrate);
                    },
                    "Migrate state for keygroup " + keyGroup
            );
            transmitComplete.join();
            allToMigrate.remove(keyGroup);
        }
        scaleMailConsumer.accept(
                scalingContext::tryCompleteScale,
                "Notify state migration completed"
        );
    }


    void notifyStatesMigratedReady(List<Integer> requestedStates) {
        synchronized (availableLock){
            LOG.info("Notify states {} are available in Thread {}",
                    requestedStates,
                    Thread.currentThread().getName());
            available.addAll(requestedStates);
            availableLock.notifyAll();
        }
    }

    void mergeStateOrBin(
            ScaleBuffer.StateBuffer stateBuffer,
            MigrationBuffer migrationBuffer,
            StreamOperator mainOperator,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling,
            AvailabilityProvider.AvailabilityHelper availabilityHelper,
            AtomicBoolean cancel,
            @Nullable Runnable mergedNotifier) {
        checkNotNull(mainOperator, "Main operator is not set yet in task " + scalingContext.getTaskName());
        KeyOrStateID keyOrStateID = (KeyOrStateID) stateBuffer.outgoingManagedKeyedState.keySet().iterator().next();
        scaleMailConsumer.accept(
                () -> {
                    if (cancel.get()){
                        // already cancelled
                        LOG.info("already merged {}, skip...", keyOrStateID);
                        return;
                    }
                    LOG.info("merge {} in Thread {}",
                            stateBuffer,
                            Thread.currentThread().getName());
                    BitSet binFlags = mainOperator.mergeState(stateBuffer);

                    scalingContext.onMerge(keyOrStateID, binFlags);
                    if (migrationBuffer.notifyStateOrBinMerged(
                            scalingContext,
                            recordProcessorInScaling)){

                        LOG.info("Resume suspend due to {} merged", keyOrStateID);
                        CompletableFuture toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        toNotify.complete(null);
                        scalingContext.tryCompleteScale();
                    }
                    if (mergedNotifier != null){
                        mergedNotifier.run();
                    }
                },
                "Merge states"
        );
    }


    // in main thread
    void fetch(HierarchicalStateID binID, int remoteTask) {
        LOG.info("Fetch bin {} from task {}", binID, remoteTask);
        CompletableFuture.runAsync(
                () -> {
                    try {
                        scaleCommOutAdapter.sendEvent(
                                remoteTask,
                                new ScaleEvent.FetchRequest(scalingContext.getSubtaskIndex(), binID));
                    } catch (InterruptedException e) {
                        LOG.error("Failed to send fetch bin event.", e);
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    void migrateInMainThread(int targetTaskIndex, KeyOrStateID keyOrStateID) {

        LOG.info("Start extract {} {} to task {}",
                keyOrStateID.isKey() ? "key" : "bin",
                keyOrStateID,
                targetTaskIndex);

        boolean noMoreBins = scalingContext.isTimeServiceClearNeeded(keyOrStateID); // key or last bin

        try {
            ScaleBuffer.StateBuffer stateBuffer = mainOperator.collectState(keyOrStateID, noMoreBins);
            scalingContext.startMigrate(keyOrStateID, noMoreBins, targetTaskIndex);

            CompletableFuture.runAsync(
                    () ->{
                        try{
                            if (stateBuffer != ScaleBuffer.StateBuffer.EMPTY_STATE_BUFFER){
                                LOG.info(
                                        "send state {} to task {}",
                                        keyOrStateID,
                                        targetTaskIndex);
                                scaleCommOutAdapter.sendBuffer(targetTaskIndex, stateBuffer);
                            }else{
                                // only when all bins of migrated key has been fetched
                                checkState(keyOrStateID.isKey(), "State buffer is empty but not key.");
                                stateTransmitCompletes.remove(keyOrStateID.getKey()).complete(null);
                                int count =  scalingContext.transferringCount.decrementAndGet();
                                if (count == 0){
                                    scaleMailConsumer.accept(
                                            scalingContext::tryCompleteScale,
                                            "Notify state migration completed"
                                    );
                                }
                            }
                        } catch (InterruptedException e) {
                            LOG.error("Failed to send state buffer.", e);
                            throw new RuntimeException(e);
                        }
                    }
            );
        } catch (IOException e) {
            LOG.error("Failed to collect state buffer.", e);
            throw new RuntimeException(e);
        }
    }

}
