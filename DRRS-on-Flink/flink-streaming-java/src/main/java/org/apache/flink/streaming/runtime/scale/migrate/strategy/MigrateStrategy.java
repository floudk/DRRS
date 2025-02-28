package org.apache.flink.streaming.runtime.scale.migrate.strategy;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.coordinator.SubtaskScaleResourceReleaser;
import org.apache.flink.runtime.scale.io.ScaleCommListener;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.io.message.local.StateBuffer;
import org.apache.flink.runtime.scale.io.message.barrier.ConfirmBarrier;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.scale.migrate.strategy.reroute.DisableSubscaleMigrate;
import org.apache.flink.streaming.runtime.scale.scheduling.IntraSubscaleKeyOrderSelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
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


    protected MigrateStrategy(
            StreamOperator mainOperator,
            ScaleCommOutAdapter scaleCommOutAdapter,
            ScaleCommListener scaleCommListener,
            ScalingContext scalingContext,
            int targetChannelCount,
            BiConsumer<RunnableWithException,String> scaleMailConsumer,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordProcessorInScaling) {

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
            CompletableFuture<Void> consumeFuture,
            int subscaleID) {
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
                            LOG.info(
                                    "{}: send state {} to task {}",
                                    taskName,
                                    keyGroups,
                                    targetTaskIndex);
                            scaleCommOutAdapter.sendStateBuffer(
                                    targetTaskIndex,
                                    stateBuffer,
                                    subscaleID);
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
    protected abstract void mergeState(StateBuffer stateBuffer, int fromTaskIndex);

    // This function will be invoked in main thread
    public abstract void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo) throws IOException;
    public abstract void onConfirmBarrier(ConfirmBarrier cb, InputChannelInfo channelInfo);

    public abstract void processEvent(ScaleEvent event, Channel channel);
    public void processBuffer(ScaleBuffer buffer, int fromTaskIndex){
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
        if (!ScaleConfig.Instance.ENABLE_DR){
            LOG.info("Disable DR migrate");
            return new DisableDRMigrate(
                    mainOperator,
                    scaleCommOutAdapter,
                    scaleCommListener,
                    scalingContext,
                    inputGates,
                    mailboxProcessor,
                    recordConsumer);
        }else if (!ScaleConfig.Instance.ENABLE_SUBSCALE){
            LOG.info("Disable subscale migrate");
            return new DisableSubscaleMigrate(
                    mainOperator,
                    scaleCommOutAdapter,
                    scaleCommListener,
                    scalingContext,
                    inputGates,
                    mailboxProcessor,
                    recordConsumer);
        }else{
            return new FluidMigrate(
                    mainOperator,
                    scaleCommOutAdapter,
                    scaleCommListener,
                    scalingContext,
                    inputGates,
                    mailboxProcessor,
                    recordConsumer);
        }
    }

    public void onCompleteBarrier(SubtaskScaleResourceReleaser releaser) {
        throw new IllegalArgumentException("Should not call this base method");
    }

    public void close(){

    }
}
