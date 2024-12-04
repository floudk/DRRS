/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package org.apache.flink.runtime.scale.coordinator;

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.state.FlexibleKeyGroupRange;
import org.apache.flink.runtime.scale.state.migrate.MigrateStrategyMode;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.createKeyGroupPartitions;
import static org.apache.flink.runtime.io.network.ConnectionID.createScaleConnectionID;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * The scale info of a {@link org.apache.flink.runtime.jobgraph.JobVertex}.
 */
public class ScaleContext {
    private static final Logger LOG = LoggerFactory.getLogger(ScaleCoordinator.class);

    private final ExecutionJobVertex executionJobVertex;
    private final int scaleFromParallelism;
    private final int scaleToParallelism;
    private final MigrateStrategyMode migrateStrategyMode;

    private final Map<ExecutionJobVertex, IntermediateResult> upstreamJobVertices = new HashMap<>();
    private final Map<ExecutionJobVertex, IntermediateResult> downstreamJobVertices = new HashMap<>();

    private List<FlexibleKeyGroupRange> newKeyGroupPartitions = null;
    private List<FlexibleKeyGroupRange> oldKeyGroupPartitions;
    private final List<ConnectionID> connectionIDS = new ArrayList<>();

    public CompletableFuture<Void> resetFuture;

    public ScaleContext(
            ExecutionJobVertex jobVertex, int scaleToParallelism, MigrateStrategyMode strategy){
        executionJobVertex = jobVertex;
        this.scaleFromParallelism = jobVertex.getParallelism();
        this.scaleToParallelism = scaleToParallelism;
        this.migrateStrategyMode = strategy;
        jobVertex.getInputs().forEach(
                (intermediateResult) -> upstreamJobVertices.put(intermediateResult.getProducer(), intermediateResult)
        );
        Arrays.stream(jobVertex.getProducedDataSets()).forEach(
                (intermediateResult) -> {
                    List<JobVertexID> jobVertexIDS = intermediateResult.getConsumerVertices();
                    checkArgument(jobVertexIDS.size() == 1,
                            "Expect only one consumer for each intermediate result,"
                                    + " but got %s", jobVertexIDS.size());
                    ExecutionJobVertex consumer = jobVertex.getGraph().getJobVertex(jobVertexIDS.get(0));
                    downstreamJobVertices.put(consumer, intermediateResult);
                }
        );
        jobVertex.scaleContext = this;

        // Initialize the old key group partitions
        //  For simplicity, we just calculate it based on the old parallelism and baseline partition method
        List<KeyGroupRange> keyGroupPartitions =
                createKeyGroupPartitions(executionJobVertex.getMaxParallelism(),scaleFromParallelism);
        oldKeyGroupPartitions = keyGroupPartitions.stream()
                .map(keyGroupPartition-> (FlexibleKeyGroupRange) FlexibleKeyGroupRange.fromKeyGroupRange(keyGroupPartition))
                .collect(Collectors.toList());
    }

    //============================  Getter ======================================
    public String getJobVertexName() {
        return executionJobVertex.getName();
    }
    public int getNewParallelism() {
        return scaleToParallelism;
    }
    public int getOldParallelism() {
        return scaleFromParallelism;
    }
    public MigrateStrategyMode getMigrateStrategy() {
        return migrateStrategyMode;
    }
    public ExecutionJobVertex getJobVertex() {
        return executionJobVertex;
    }
    public ExecutionVertex[] getTaskVertices() {
        return executionJobVertex.getTaskVertices();
    }
    public boolean isScaleOut() {
        return scaleToParallelism > scaleFromParallelism;
    }
    public JobVertexID getJobVertexID() {
        return executionJobVertex.getJobVertexId();
    }
    public int getMaxParallelism(){
        return executionJobVertex.getMaxParallelism();
    }
    public Map<ExecutionJobVertex, IntermediateResult> getUpstreamJobVertices() {
        return upstreamJobVertices;
    }
    public Map<ExecutionJobVertex, IntermediateResult> getDownstreamJobVertices() {
        return downstreamJobVertices;
    }


    public void setNewKeyGroupPartitions(List<FlexibleKeyGroupRange> partitions) {
        newKeyGroupPartitions = checkNotNull(partitions);
    }

    public List<FlexibleKeyGroupRange> getNewKeyGroupPartitions() {
        return newKeyGroupPartitions;
    }
    public List<FlexibleKeyGroupRange> getOldKeyGroupPartitions() {
        return oldKeyGroupPartitions;
    }

    public List<ConnectionID> getConnectionIDS(){
        if (connectionIDS.isEmpty()) {
            ExecutionVertex[] taskVertices = executionJobVertex.getTaskVertices();
            checkArgument(taskVertices.length == scaleToParallelism,
                    "The taskVertices size "+executionJobVertex.getTaskVertices().length +
                            " is not equal to the total parallelism "+scaleToParallelism);

            for (ExecutionVertex taskVertex : taskVertices) {
                TaskManagerLocation taskManagerLocation = taskVertex.getCurrentAssignedResourceLocation();
                if (taskManagerLocation == null) {
                    LOG.error("The taskManagerLocation of task {} is null", taskVertex.getParallelSubtaskIndex());
                    throw new IllegalStateException("The taskManagerLocation of task "+taskVertex.getParallelSubtaskIndex()+" is null");
                }
                int port = 31425;
                try{
                    port = ScaleConfig.Instance.SCALE_PORT;
                } catch (Exception e){
                    LOG.error("The scale port is not set since {}", e.getMessage());
                }
                connectionIDS.add(createScaleConnectionID(taskManagerLocation, port));
            }
        }
        return connectionIDS;
    }
}
