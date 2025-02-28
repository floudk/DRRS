/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.scale;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelectorRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.MultipleRecordWriters;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.SingleRecordWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.coordinator.SubtaskScaleCoordinator;
import org.apache.flink.runtime.scale.coordinator.SubtaskScaleResourceReleaser;
import org.apache.flink.runtime.scale.io.ScaleCommListener;
import org.apache.flink.runtime.scale.io.message.barrier.ScaleBarrier;
import org.apache.flink.runtime.scale.io.message.barrier.TriggerBarrier;
import org.apache.flink.runtime.scale.io.ScaleCommManager;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.scale.migrate.strategy.MigrateStrategy;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class SubtaskSubscaleHandler<OUT> {
    protected static final Logger LOG = LoggerFactory.getLogger(SubtaskSubscaleHandler.class);


    private final TaskInfo taskInfo;
    private final JobVertexID jobVertexID;
    private final int subtaskIndex;

    private final SubtaskScaleCoordinator subtaskScaleCoordinator;

    private StreamOperator mainOperator;
    private final MailboxProcessor mailboxProcessor;
    private final MailboxExecutor mainMailboxExecutor;

    private ThrowingBiConsumer<StreamRecord, InputChannelInfo ,Exception> scalingRecordConsumer;
    private final Runnable runningChecker;

    private final ScalingContext scalingContext;

    // this objects is kind of a read-only object, since the real holder is keyContext
    private MigrateStrategy migrateStrategy;


    private final ScaleCommManager scaleCommManager;
    private final ScaleCommListener scaleCommListener;


    //------------------- upstream specific -------------------
    private RecordWriter upstreamRecordWriter;
    //----------------------------------------------------------

    private CompletableFuture<Void> initFuture = new CompletableFuture<>();

    public SubtaskSubscaleHandler(
            TaskInfo taskInfo,
            SubtaskScaleCoordinator subtaskScaleCoordinator,
            MailboxProcessor mailboxProcessor,
            MailboxExecutor mailboxExecutor,
            Runnable runningChecker) {
        this.taskInfo = checkNotNull(taskInfo);
        this.subtaskScaleCoordinator = checkNotNull(subtaskScaleCoordinator);
        this.mailboxProcessor =  checkNotNull(mailboxProcessor);
        this.mainMailboxExecutor = checkNotNull(mailboxExecutor);
        this.runningChecker = checkNotNull(runningChecker);

        this.jobVertexID = subtaskScaleCoordinator.getJobVertexID();
        this.subtaskIndex = subtaskScaleCoordinator.getSubtaskIndex();

        this.scaleCommManager = subtaskScaleCoordinator.getScaleConsumerManager();
        this.scaleCommListener = new ScaleCommListener();

        this.scalingContext = subtaskScaleCoordinator.scalingContext;

    }

    // invoked by #StreamTask.invoke() to initialize the subscale handler
    // this initialization is invoked only once during the whole task lifecycle
    public void initHandler(
            StreamOperator mainOperator,
            @Nullable ThrowingBiConsumer<StreamRecord, InputChannelInfo ,Exception> scalingRecordConsumer,
            @Nullable TypeSerializer inputSerializer) {

        this.mainOperator = checkNotNull(mainOperator);
        this.scalingRecordConsumer = scalingRecordConsumer;

        final String taskName = taskInfo.getTaskNameWithSubtasks();

        LOG.info("{}: init the subscale handler ", taskName);

        if (inputSerializer != null) {
            // do not consider the scaling of source operator
            scaleCommManager.registerListener(
                    jobVertexID,
                    subtaskIndex,
                    scaleCommListener,
                    inputSerializer,
                    mainOperator::createStateMap,
                    mainOperator::getSerializers,
                    mainOperator::getStateSnapshotTransformer);
        }
        initFuture.complete(null);
    }

    // -------------------------- 0. update deploy --------------------------
    public void expandRecordWriters(RecordWriter recordWriter){
        recordWriter.expand();
        this.upstreamRecordWriter = recordWriter;
    }

    // -------------------------- 1. initialize scale --------------------------

    /**
     * After reset, the system should be ready to handle subscale,
     * and there may be some states migration come before the subscale trigger
     * <p>
     * This method is running not in main thread
     * @param stateMigrateSelector
     * @param inputSerializer
     * @throws IOException
     */
    public void reset(IndexedInputGate[] inputGates) throws IOException {
        initFuture.join(); 
        checkNotNull(scalingRecordConsumer, "Scaling record consumer is null");
        checkNotNull(mainOperator, "Main operator is null");

        this.migrateStrategy = MigrateStrategy.createMigrateStrategy(
                mainOperator,
                subtaskScaleCoordinator.getScaleCommAdapter(),
                scaleCommListener,
                subtaskScaleCoordinator.scalingContext,
                inputGates,
                mailboxProcessor::sendScaleMail,
                scalingRecordConsumer);

        mailboxProcessor.sendScaleMail(
                () -> scalingContext.triggered(mainOperator.getFlexibleKeyGroupRange()),
                "Init scaling context"
        );
        scaleCommManager.startConnection();
    }

    // ---------------------------- 2. trigger subscale in upstream ----------------------------
    public void triggerSubscale(Map<Integer,Integer> newKeyPartition, RecordWriterDelegate recordWriter) {
        LOG.info("{}: trigger subscale with new key partition: {}",
                taskInfo.getTaskNameWithSubtasks(),
                newKeyPartition);
        if (newKeyPartition.isEmpty()) {
            finishSubscale(recordWriter);
            return;
        }

        mainMailboxExecutor.execute(
                ()->{
                    FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
                    runningChecker.run();
                    final Map<Integer,Integer> currentRoutingTable = getCurrentAndUpdateRoutingTable(newKeyPartition);
                    LOG.info("{} Send trigger barriers,current {}, new {}",
                            taskInfo.getTaskNameWithSubtasks(),
                            currentRoutingTable,
                            newKeyPartition);

                    TriggerBarrier triggerBarrier =
                            new TriggerBarrier(newKeyPartition, currentRoutingTable);

                    // ConfirmBarrier confirmBarrier = new ConfirmBarrier(newKeyPartition.keySet());

                    // as non-primary event
                    if ( recordWriter instanceof SingleRecordWriter) {
                        recordWriter.getRecordWriter(0).emitTriggerBarrier(triggerBarrier);
                    }else if(recordWriter instanceof MultipleRecordWriters){
                        // need to find the record writer of scaling partition
                        throw new UnsupportedOperationException(
                                "Not implemented in class " + getClass().getName());
                    }
                },
                "Trigger barriers");
    }

    // no more subscale will be triggered unless next reset
    private void finishSubscale(RecordWriterDelegate recordWriter) {
        LOG.info("{}: finish subscale: no more subscale will be triggered",
                taskInfo.getTaskNameWithSubtasks());
        mainMailboxExecutor.execute(
                ()->{
                    FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
                    runningChecker.run();
                    ScaleBarrier.CompleteBarrier completeBarrier = new ScaleBarrier.CompleteBarrier();
                    if ( recordWriter instanceof SingleRecordWriter) {
                        recordWriter.getRecordWriter(0).emitCompleteBarrier(completeBarrier);
                    }else if(recordWriter instanceof MultipleRecordWriters){
                        // need to find the record writer of scaling partition
                        throw new UnsupportedOperationException(
                                "Not implemented in class " + getClass().getName());
                    }
                },
                "Finish subscale"
        );
    }


    // ---------------------------- 3. react to subscale ----------------------------

    /**
     * This method is running in main thread
     * @param tb: trigger barrier
     * @param channelInfo: input channel info( may be useless)
     */
    public void onTriggerBarrier(TriggerBarrier tb, InputChannelInfo channelInfo) throws IOException {
        LOG.info("{} Received trigger barrier from channel {}.", taskInfo.getTaskNameWithSubtasks(), channelInfo);
        migrateStrategy.onTriggerBarrier(tb, channelInfo);
    }
    public void onCompleteBarrier() {
        SubtaskScaleResourceReleaser releaser = subtaskScaleCoordinator.resourceReleaser;
        releaser.registerReleaseCallback(this::release);
        migrateStrategy.onCompleteBarrier(releaser);
    }


    public <IN> void processRecordOnScaling(
            StreamRecord<IN> record,
            InputChannelInfo channelInfo,
            int keyGroupIndex,
            Counter numRecordsIn,
            Input<IN> input) throws Exception {
        migrateStrategy.processRecord(record, channelInfo, keyGroupIndex, numRecordsIn, input);
    }

    public void processScaleMarker(ScaleEvaEvent scaleEvaEvent) throws Exception {
        checkState(!taskInfo.getTaskName().toLowerCase().contains("sink"),
                "Sink operator should not receive scale marker");
        // LOG.info("Transform scale marker to latency marker: {}", scaleEvaEvent);
        LatencyMarker marker = new LatencyMarker(scaleEvaEvent.creationTime, taskInfo.getTaskName());
        checkArgument(mainOperator instanceof AbstractStreamOperator,
                "Not supported operator type: " + mainOperator.getClass().getName());
        ((AbstractStreamOperator) mainOperator).processLatencyMarker(marker);
    }


    public boolean isAvailable() {
        return migrateStrategy == null || migrateStrategy.isAvailable();
    }
    public CompletableFuture<?> getAvailableFuture() {
        return migrateStrategy.getAvailableFuture();
    }



    // --------------------- State Migrate Strategy Related --------------------------

    public boolean isScaling() {
        return subtaskScaleCoordinator.isScaling();
    }


    public CompletableFuture<Void> getScaleTerminationCondition() {
        return subtaskScaleCoordinator.getScaleTerminationCondition();
    }

    // clean up necessary resources to wait for the next scale
    public void release(){
        scaleCommManager.stopConnection();
    }


    // -------------------------- Upstream Utils --------------------------

    /**
     * Get routing table from record writer
     * @return partial routing table (only related keys)
     */
    private Map<Integer,Integer> getCurrentAndUpdateRoutingTable(Map<Integer,Integer> newKeyPartitions){
        checkNotNull(upstreamRecordWriter, "Record writer is not set yet");
        if (upstreamRecordWriter instanceof ChannelSelectorRecordWriter){
            return ((ChannelSelectorRecordWriter) upstreamRecordWriter).getAndUpdateRoutingTable(newKeyPartitions);
        } else{
            throw new RuntimeException(
                    "Not supported record writer type: " + upstreamRecordWriter.getClass().getName());
        }
    }
}
