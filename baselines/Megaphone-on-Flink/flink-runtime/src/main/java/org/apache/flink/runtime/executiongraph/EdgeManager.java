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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scale.coordinator.ScaleCoordinator;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Class that manages all the connections between tasks. */
public class EdgeManager {

    //only for debug
    static final Logger LOG = LoggerFactory.getLogger(ScaleCoordinator.class);

    private final Map<IntermediateResultPartitionID, List<ConsumerVertexGroup>> partitionConsumers =
            new HashMap<>();

    private final Map<ExecutionVertexID, List<ConsumedPartitionGroup>> vertexConsumedPartitions =
            new HashMap<>();

    private final Map<IntermediateResultPartitionID, List<ConsumedPartitionGroup>>
            consumedPartitionsById = new HashMap<>();

    public void connectPartitionWithConsumerVertexGroup(
            IntermediateResultPartitionID resultPartitionId,
            ConsumerVertexGroup consumerVertexGroup) {

        checkNotNull(consumerVertexGroup);

        List<ConsumerVertexGroup> groups =
                getConsumerVertexGroupsForPartitionInternal(resultPartitionId);
        groups.add(consumerVertexGroup);
    }

    public void connectVertexWithConsumedPartitionGroup(
            ExecutionVertexID executionVertexId, ConsumedPartitionGroup consumedPartitionGroup) {

        checkNotNull(consumedPartitionGroup);

        final List<ConsumedPartitionGroup> consumedPartitions =
                getConsumedPartitionGroupsForVertexInternal(executionVertexId);

        consumedPartitions.add(consumedPartitionGroup);
    }

    private List<ConsumerVertexGroup> getConsumerVertexGroupsForPartitionInternal(
            IntermediateResultPartitionID resultPartitionId) {
        return partitionConsumers.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertexInternal(
            ExecutionVertexID executionVertexId) {
        return vertexConsumedPartitions.computeIfAbsent(executionVertexId, id -> new ArrayList<>());
    }

    public List<ConsumerVertexGroup> getConsumerVertexGroupsForPartition(
            IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(
                getConsumerVertexGroupsForPartitionInternal(resultPartitionId));
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertex(
            ExecutionVertexID executionVertexId) {
        return Collections.unmodifiableList(
                getConsumedPartitionGroupsForVertexInternal(executionVertexId));
    }

    public void registerConsumedPartitionGroup(ConsumedPartitionGroup group) {
        for (IntermediateResultPartitionID partitionId : group) {
            consumedPartitionsById
                    .computeIfAbsent(partitionId, ignore -> new ArrayList<>())
                    .add(group);
        }
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsByIdInternal(
            IntermediateResultPartitionID resultPartitionId) {
        return consumedPartitionsById.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsById(
            IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(
                getConsumedPartitionGroupsByIdInternal(resultPartitionId));
    }

    public int getNumberOfConsumedPartitionGroupsById(
            IntermediateResultPartitionID resultPartitionId) {
        return getConsumedPartitionGroupsByIdInternal(resultPartitionId).size();
    }

    // --------------------------------------------------------------------------------------------
    // Scale Utils
    // --------------------------------------------------------------------------------------------

    public void updateConnection(
            ExecutionJobVertex ejv,
            IntermediateResult ir,
            JobVertexInputInfo jobVertexInputInfo){
        for(IntermediateResultPartition irp:ir.getPartitions()){
            partitionConsumers.remove(irp.getPartitionId());
            consumedPartitionsById.remove(irp.getPartitionId());
        }
        EdgeManagerBuildUtil.connectPointwise(ejv,ir,jobVertexInputInfo);
    }

    public void clearVertexConsumedPartitions(ExecutionJobVertex ejv){
        Arrays.stream(ejv.getTaskVertices()).map(ExecutionVertex::getID)
                .forEach(vertexConsumedPartitions::remove);
    }
}
