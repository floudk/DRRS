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

package org.apache.flink.runtime.scale.coordinator;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.ScalableTask;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.deploy.DownstreamTaskDeployUpdateDescriptor;
import org.apache.flink.runtime.scale.io.ScaleCommManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class SubtaskScaleCoordinator implements Closeable {
    protected static final Logger LOG = LoggerFactory.getLogger(SubtaskScaleCoordinator.class);

    private final String taskName;
    private final JobVertexID jobVertexID;
    private final int subtaskIndex;

    private CompletableFuture<Void> scaleTerminationCondition;

    public final ScalingContext scalingContext;

    private final ScaleCommOutAdapter scaleCommOutAdapter;

    private final ScaleCommManager scaleCommManager;

    public SubtaskScaleResourceReleaser resourceReleaser;

    ScalableTask streamTask = null;

    public SubtaskScaleCoordinator(
            TaskInfo taskInfo,
            ScaleCommManager scaleCommManager,
            JobVertexID jobVertexID,
            int subtaskIndex) {
        this.taskName = taskInfo.getTaskNameWithSubtasks();
        this.jobVertexID = jobVertexID;
        this.subtaskIndex = subtaskIndex;

        this.scalingContext = new ScalingContext(taskInfo);

        this.scaleTerminationCondition = CompletableFuture.completedFuture(null);
        this.scaleCommManager = scaleCommManager;
        this.scaleCommOutAdapter = new ScaleCommOutAdapter(subtaskIndex, jobVertexID, scaleCommManager);
    }

    public void setInvokable(ScalableTask streamTask){
        this.streamTask = streamTask;
    }

    /**
     * reset the subtask scale coordinator for new scale operation
     */
    public void reset(
            int newParallelism,
            List<ConnectionID> connectionIDs,
            Runnable ScaleCompletionAcknowledger) throws IOException {
        LOG.info("{} reset subtask scale coordinator with new parallelism {}",
                taskName, newParallelism);
        this.scaleTerminationCondition = new CompletableFuture<>();
        scalingContext.updateTaskInfo(newParallelism);
        scaleCommOutAdapter.reset(connectionIDs);
        streamTask.resetScale();
        resourceReleaser = new SubtaskScaleResourceReleaser(ScaleCompletionAcknowledger);
        resourceReleaser.registerReleaseCallback(this::release);
    }

    /**
     * release the resources allocated for current scale operation
     */
    public void release() {
        this.scaleTerminationCondition.complete(null);
    }


    @Override
    public void close() throws IOException { }
    // ----------------- getters -----------------
    public boolean isScaling() {
        return scalingContext.isScaling();
    }
    public CompletableFuture<Void> getScaleTerminationCondition() {
        return scaleTerminationCondition;
    }
    public String getTaskName(){
        return taskName;
    }
    public int getSubtaskIndex() {
        return subtaskIndex;
    }
    public JobVertexID getJobVertexID() {
        return jobVertexID;
    }
    public ScalingContext getScaleStatusHolder() {
        return scalingContext;
    }
    public ScaleCommManager getScaleConsumerManager() {
        return scaleCommManager;
    }
    public ScaleCommOutAdapter getScaleCommAdapter() {
        return checkNotNull(scaleCommOutAdapter);
    }


    // for operators that are as the upstream of the scale operator
    public void expandResultPartition(
            IntermediateResultPartitionID partitionID,
            int newNumSubPartitions,
            BiFunction<Integer, ResultPartitionType, Pair<Integer,Integer>> minMaxPairSupplier,
            ResultPartitionWriter[] partitionWriters) {
        for(ResultPartitionWriter writer : partitionWriters) {
            if (writer.getPartitionId().getPartitionId().equals(partitionID)) {
                ResultPartitionType type = writer.getPartitionType();
                Pair<Integer,Integer> minMaxPair =
                        minMaxPairSupplier.apply(newNumSubPartitions, type);
                writer.updateNumberOfSubpartitions(newNumSubPartitions,minMaxPair);
                streamTask.expandRecordWriters(writer.getPartitionId());
            }
        }

    }

    // for operators that are as the downstream of the scale operator
    public void expandInputGate(
            SingleInputGate gate,
            DownstreamTaskDeployUpdateDescriptor downstreamTaskDeployUpdateDescriptor,
            SingleInputGateFactory factory) throws Exception {
        InputChannel[] addedInputChannel =
                factory.getAddedInputChannels(gate, downstreamTaskDeployUpdateDescriptor);

        gate.addGateChannels(addedInputChannel);
        streamTask.expandInputGates(gate, Arrays.asList(addedInputChannel));
    }

    // invoked by the upstream operator of the scaled one
    public void triggerScale(
            JobVertexID scalingJobVertexID,
            Set<JobVertexID> upstreamJobVertexIDs,
            Map<Integer,Integer> involvedKeyGroups) {
        if(scalingJobVertexID==null && upstreamJobVertexIDs==null && involvedKeyGroups==null) {
            LOG.info("{} invoke release", taskName);
            invokeRelease();
        }else{
            streamTask.triggerScale(
                    scalingJobVertexID, upstreamJobVertexIDs, involvedKeyGroups);
        }
    }

    public void invokeRelease(){
        resourceReleaser.release();
    }
}
