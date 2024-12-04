package org.apache.flink.streaming.runtime.scale.migrate.strategy.reroute;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.io.ScaleCommListener;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.RerouteCache;
import org.apache.flink.runtime.scale.io.message.barrier.ConfirmBarrier;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.scale.migrate.strategy.FluidMigrate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DisableSubscaleMigrate extends FluidMigrate {
    private final RerouteHelper rerouteManager;

    private Map<Integer,Set<Integer>> sourceTaskWithInKeysCache;

    public DisableSubscaleMigrate(
            StreamOperator mainOperator,
            ScaleCommOutAdapter scaleCommOutAdapter,
            ScaleCommListener scaleCommListener,
            ScalingContext scalingContext,
            IndexedInputGate[] inputGates,
            BiConsumer<RunnableWithException, String> poisonMailbox,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling) {
        super(
                mainOperator,
                scaleCommOutAdapter,
                scaleCommListener,
                scalingContext,
                inputGates,
                poisonMailbox,
                recordProcessorInScaling);
        rerouteManager = new RerouteHelper(scalingContext, scaleCommOutAdapter, 0);
    }

    @Override
    protected void migrateStatesAsync(int targetTaskIndex, List<Integer> requestedStates, int subscaleID){
        CompletableFuture.runAsync(() -> {

            requestedStates.forEach(
                    (keygroup)->{
                        try {
                            CompletableFuture<Void> transmitComplete = new CompletableFuture<>();
                            stateTransmitCompletes.put(keygroup, transmitComplete);
                            final List<Integer> keygroups = List.of(keygroup);
                            CompletableFuture<Void> adapterConsumeFuture = new CompletableFuture<>();
                            collectAndMigrateStates(
                                    targetTaskIndex, keygroups, adapterConsumeFuture, subscaleID);
                            rerouteManager.setStateConsumeFuture(keygroup, adapterConsumeFuture);
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
    public void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo) throws IOException {
        if(sourceTaskWithInKeysCache == null) {
            sourceTaskWithInKeysCache = new HashMap<>();
            tb.getInKeys(subtaskIndex).forEach(
                    (sourceTask, inKeys) -> sourceTaskWithInKeysCache.put(sourceTask, new HashSet<>(inKeys))
            );
        }
        super.onTriggerBarrier(tb, channelInfo);
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

        // create if not exist
        rerouteManager.rerouteConfirmBarrier(cb, targetTasks, channelInfo, confirmBarrierConsumedFuture);

        LOG.info("Reroute confirm barrier {} ({}) to target tasks {}: {}/{}",
                cb, cb.getInvolvedKeys(), targetTasks, reroutedConfirmBarriers.get(cb), targetChannelCount);
        Set<Integer> confirmedKeyGroups = new HashSet<>();

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
                        rerouteManager.noMoreBarriersFuture.complete(null);
                    }
            );
        }
    }
    @Override
    protected Set<Integer> getConfirmedKeys(Set<Integer> confirmedKeys, int fromTask){
        Set<Integer> inKeys = sourceTaskWithInKeysCache.get(fromTask);
        checkNotNull(inKeys, "inKeys should not be null: %s, %s", fromTask, sourceTaskWithInKeysCache);
        // get the intersection of confirmedKeys and inKeys
        Set<Integer> involvedKeys = new HashSet<>(confirmedKeys);
        involvedKeys.retainAll(inKeys);
        return involvedKeys;
    }

    @Override
    protected void reroute(StreamRecord record, int keyGroupIndex, int subscaleID){
        rerouteManager.rerouteRecord(record, keyGroupIndex);
    }

    private class RerouteHelper extends RerouteManager{
        Map<Integer, RerouteCache> rerouteBuffers = new ConcurrentHashMap<>();
        Map<Integer, Integer> rerouteCounts = new ConcurrentHashMap<>();

        public RerouteHelper( ScalingContext scalingContext,
                              ScaleCommOutAdapter scaleCommOutAdapter,
                              int subscaleID){
            super(scalingContext, scaleCommOutAdapter, subscaleID);
        }

        @Override
        public void registerClose() {
            noMoreBarriersFuture.thenRun(() -> {
                // Wait for all tasks to complete
                while (pendingTasks.get() > 0) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted while waiting for reroute tasks to complete", e);
                        Thread.currentThread().interrupt();
                    }
                }
                for (RerouteCache rerouteCache : rerouteBuffers.values()) {
                    rerouteCache.shutdownScheduler();
                }
                taskExecutor.interrupt(); // Stop the task executor thread
            });
        }

        @Override
        protected void rerouteCBInternal(
                int targetTask,
                InputChannelInfo channelInfo,
                ConfirmBarrier cb,
                CompletableFuture<Void> consumeFuture) {
            RerouteCache cacheToReroute = rerouteBuffers.remove(channelInfo.getGateIdx());
            reroute(targetTask, cacheToReroute, channelInfo, cb, consumeFuture);
        }

        @Override
        protected void rerouteRecordInternal(StreamRecord record, int targetTask, int keyGroupIndex) {
            RerouteCache rerouteBuffer =
                    rerouteBuffers.computeIfAbsent(keyGroupIndex, k -> createNewRerouteBuffer(targetTask));
            if (!rerouteBuffer.addRecord(record)){
                RerouteCache newCache = createNewRerouteBuffer(targetTask);
                newCache.initWithPreviousOverflow(record, rerouteBuffer);
                newCache.involvedKeys.add(keyGroupIndex);
                rerouteBuffers.put(keyGroupIndex, newCache);
                reroute(targetTask, rerouteBuffer, null, null, null);
            }else{
                rerouteBuffer.involvedKeys.add(keyGroupIndex);
            }
        }

        @Override
        protected RerouteCache createNewRerouteBuffer(int targetTaskIndex) {
            return new RemoteRerouteCache(
                    targetTaskIndex,
                    () -> addTask(() -> {
                        reroute(targetTaskIndex, rerouteBuffers.remove(targetTaskIndex), null, null, null);}),
                    ScaleConfig.Instance.REROUTE_BUFFER_TIMEOUT,
                    ScaleConfig.Instance.REROUTE_BUFFER_REMOTE_SIZE,
                    scaleCommOutAdapter.getByteBufAllocator(),
                    scaleCommOutAdapter.getInputSerializer(scalingContext.getSubtaskIndex()).duplicate());

        }

        @Override
        protected int getAndIncrementRerouteCount(int targetTask) {
            int count = rerouteCounts.getOrDefault(targetTask, 0);
            rerouteCounts.put(targetTask, count + 1);
            return count;
        }
    }

}
