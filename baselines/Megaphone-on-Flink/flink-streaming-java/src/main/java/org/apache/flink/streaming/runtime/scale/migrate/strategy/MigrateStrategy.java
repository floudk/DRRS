package org.apache.flink.streaming.runtime.scale.migrate.strategy;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.AvailabilityProvider;
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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class MigrateStrategy implements AvailabilityProvider {
    protected static final Logger LOG = LoggerFactory.getLogger(MigrateStrategy.class);

    protected final String taskName;
    protected final int subtaskIndex;
    protected final ScalingContext scalingContext;

    protected final ScaleCommOutAdapter scaleCommOutAdapter;

    protected final StreamOperator mainOperator;
    protected final BiConsumer<RunnableWithException,String> scaleMailConsumer;

    protected final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    protected final int targetChannelCount;

    protected final ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling;


    protected final Map<Integer, CompletableFuture<Void>> prepareStateTransmitCompletes = new HashMap<>();

    protected MigrateStrategy(
            StreamOperator mainOperator,
            ScaleCommOutAdapter scaleCommOutAdapter,
            ScaleCommListener scaleCommListener,
            ScalingContext scalingContext,
            int targetChannelCount,
            BiConsumer<RunnableWithException,String> scaleMailConsumer,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling){

        this.mainOperator = checkNotNull(mainOperator);

        this.scaleCommOutAdapter = checkNotNull(scaleCommOutAdapter);


        this.targetChannelCount = targetChannelCount;
        this.scalingContext = scalingContext;

        this.scaleMailConsumer = scaleMailConsumer;
        this.recordProcessorInScaling = recordProcessorInScaling;

        this.taskName = scalingContext.getTaskName();
        this.subtaskIndex = scalingContext.getSubtaskIndex();

        scaleCommListener.setConsumers(this::processBuffer, this::processEvent);
    }

    // ---------------- process incoming records ----------------

    public abstract <T> void processRecord (
            StreamRecord<T> record, InputChannelInfo inputChannelInfo, int keyGroupIndex, Counter numRecordsIn, Input<T> input)
            throws Exception;

    // process incoming record as non-scaling period
    protected <T> void directProcess(
            StreamRecord<T> record,
            Counter numRecordsIn,
            Input<T> input) throws Exception {
        numRecordsIn.inc();
        input.setKeyContextElement(record);
        input.processElement(record);
    }

    @Override
    public CompletableFuture getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }



    // ---------------- process outgoing state ----------------

    protected void collectAndMigrateStates(
            int targetTaskIndex,
            List<Integer> keyGroups,
            int responseId,
            CompletableFuture<Void> consumeFuture) {

        CompletableFuture<Void> prepareFuture;
        synchronized (prepareStateTransmitCompletes){
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            keyGroups.forEach(k->{
                CompletableFuture<Void> future = prepareStateTransmitCompletes.computeIfAbsent(
                        k,
                        (keyGroup) -> new CompletableFuture<>());
                futures.add(future);
            });
            prepareFuture = FutureUtils.waitForAll(futures);
        }
        prepareFuture.thenRunAsync(()->
                collectAndMigrateInternal(
                        targetTaskIndex,
                        keyGroups,
                        responseId,
                        consumeFuture));
    }

    void collectAndMigrateInternal(
            int targetTaskIndex,
            List<Integer> keyGroups,
            int responseId,
            CompletableFuture<Void> consumeFuture) {
        LOG.info(
                "{}: async collect and migrate states {} to task {}",
                taskName,
                keyGroups,
                targetTaskIndex);
        CompletableFuture<StateBuffer> collectFuture = new CompletableFuture<>();
        scaleMailConsumer.accept(
                () -> {
                    try{
                        collectFuture.complete(mainOperator.collectState(keyGroups));
                        LOG.info(
                                "{}: collected state {} to task {}",
                                taskName,
                                keyGroups,
                                targetTaskIndex);
                    }catch (Exception e){
                        LOG.error(
                                "Failed to collect state in Thread {}",
                                Thread.currentThread().getName(),
                                e);
                        collectFuture.completeExceptionally(e);
                    }
                },
                "Collect states"
        );
        collectFuture.thenAcceptAsync(
                (stateBuffer) -> {
                    try {
                        if (!stateBuffer.isEmpty()) {
                            stateBuffer.bufferID = responseId;
                            LOG.info(
                                    "{}: send state buffer {} to task {}",
                                    taskName,
                                    responseId,
                                    targetTaskIndex);
                            scaleCommOutAdapter.sendBuffer(targetTaskIndex, stateBuffer);
                        }
                        consumeFuture.complete(null);
                    } catch (Exception e) {
                        LOG.error(
                                "Failed to migrate state to task {} in Thread {}",
                                targetTaskIndex,
                                Thread.currentThread().getName(),
                                e);
                        throw new RuntimeException();
                    }
                });
    }

    // receive and migrate in
    protected abstract void mergeState(StateBuffer stateBuffer);

    // This function will be invoked in main thread
    public abstract void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo) throws IOException;

    public void processEvent(ScaleEvent event){
        // do nothing
    }
    public void processBuffer(ScaleBuffer buffer, int fromTaskIndex){
        // do nothing
    }

    protected void migrateStatesAsync(int targetTaskIndex, List<Integer> requestedStates) {
        // do nothing
    }


    public static MigrateStrategy createMigrateStrategy(
            StreamOperator mainOperator,
            ScaleCommOutAdapter scaleCommOutAdapter,
            ScaleCommListener scaleCommListener,
            ScalingContext scalingContext,
            IndexedInputGate[] inputGates,
            BiConsumer<RunnableWithException,String> mailboxProcessor,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordConsumer){
        return new FluidMigrate(
                    mainOperator,
                    scaleCommOutAdapter,
                    scaleCommListener,
                    scalingContext,
                    inputGates,
                    mailboxProcessor,
                    recordConsumer);
    }

    public void onCompleteBarrier(SubtaskScaleResourceReleaser releaser) {
        throw new IllegalArgumentException("Should not call this base method");
    }

    public void close(){
        prepareStateTransmitCompletes.clear();
    }

}
