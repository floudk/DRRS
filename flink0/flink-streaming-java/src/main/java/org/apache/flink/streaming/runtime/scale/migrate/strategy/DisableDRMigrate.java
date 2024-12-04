package org.apache.flink.streaming.runtime.scale.migrate.strategy;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.io.ScaleCommListener;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.barrier.ConfirmBarrier;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class DisableDRMigrate extends FluidMigrate{

    public Map<TriggerBarrier, Integer> barrierCount = new HashMap<>();

    private final IndexedInputGate[] inputGates;
    private final Set<InputChannelInfo> blockedChannels = new HashSet<>();

    private final CompletableFuture<Void> alignmentFuture = new CompletableFuture<>();

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
        LOG.info("Received new trigger barrier from channel {}.",  channelInfo);
        barrierCount.put(tb, barrierCount.getOrDefault(tb, 0) + 1);

        Map<Integer, List<Integer>> sourceTaskWithInKeys = tb.getInKeys(subtaskIndex);
        Map<Integer, List<Integer>> targetTaskWithOutKeys = tb.getOutKeys(subtaskIndex);

        if (barrierCount.get(tb) == 1){
            // only react to the first barrier

            // main thread: prepare for receiving events and buffers
            scalingContext.subscale(sourceTaskWithInKeys, targetTaskWithOutKeys, tb.subscaleID);

            migrationBuffer.subscale(sourceTaskWithInKeys);
            // async request state migration in
            requestStatesAsync(sourceTaskWithInKeys,tb.subscaleID);
        }

        if (targetTaskWithOutKeys.isEmpty()){
            unblockChannel(channelInfo);
        }else{
            blockedChannels.add(channelInfo);
            if (barrierCount.get(tb) == targetChannelCount){
                // allow any migration to proceed
                alignmentFuture.complete(null);
                for (InputChannelInfo blockedChannel : blockedChannels) {
                    unblockChannel(blockedChannel);
                }
                blockedChannels.clear();
                throw new UnsupportedOperationException("Not implemented yet");
            }
        }

    }

    @Override
    protected void migrateStatesAsync(int targetTaskIndex,List<Integer> requestedStates,int subscaleID){
        alignmentFuture.thenRunAsync(()->super.migrateStatesAsync(targetTaskIndex,requestedStates,subscaleID));
    }

    @Override
    public void onConfirmBarrier(ConfirmBarrier cb, InputChannelInfo channelInfo) {
        throw new IllegalStateException("Received confirm barrier in DisableDRMigrate");
    }

    private void unblockChannel(InputChannelInfo channelInfo) throws IOException {
        inputGates[channelInfo.getGateIdx()].resumeConsumption(channelInfo);
    }



}
