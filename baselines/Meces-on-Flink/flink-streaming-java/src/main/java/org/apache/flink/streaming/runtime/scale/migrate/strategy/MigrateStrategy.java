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
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
            StreamRecord<T> record,
            InputChannelInfo inputChannelInfo,
            int keyGroupIndex,
            int subBinIndex,
            Counter numRecordsIn,
            Input<T> input)
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


    // This function will be invoked in main thread
    public abstract void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo, SubtaskScaleResourceReleaser releaser) throws IOException;

    public void processEvent(ScaleEvent event, Channel channel){
        // do nothing
    }
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

            return new FluidMigrate(
                    mainOperator,
                    scaleCommOutAdapter,
                    scaleCommListener,
                    scalingContext,
                    inputGates,
                    mailboxProcessor,
                    recordConsumer);

    }

    public void close(){

    }

    public abstract boolean trySetCurrentKey(Object key) throws Exception;
}
