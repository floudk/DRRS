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
 * limitations under the License.
 */

package org.apache.flink.runtime.scale.io.message.deploy;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scale.coordinator.ScaleContext;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;


import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public final class DownstreamTaskDeployUpdateDescriptor implements Serializable{
    private static final long serialVersionUID = 2L;

    private final IntermediateDataSetID resultId;
    private final Tuple2<Integer, Integer> parallelisms; // [old, new]

    private final ShuffleDescriptor[] shuffleDescriptors;
    private final int consumedSubpartitionIndex;

    public DownstreamTaskDeployUpdateDescriptor(
            IntermediateDataSetID dataSetID,
            Tuple2<Integer, Integer> parallelisms,
            ExecutionVertexInputInfo newExecutionVertexInputInfo,
            ShuffleDescriptorAndIndex[] inputChannels){

        this.resultId = checkNotNull(dataSetID);
        this.parallelisms = checkNotNull(parallelisms);

        final IndexRange subpartitionIndexRange =
                newExecutionVertexInputInfo.getSubpartitionIndexRange();
        checkArgument(subpartitionIndexRange.getStartIndex() == subpartitionIndexRange.getEndIndex(),
                "Expected subpartition index range to be a single index, but got %s.",
                subpartitionIndexRange);
        this.consumedSubpartitionIndex = subpartitionIndexRange.getStartIndex();

        this.shuffleDescriptors = new ShuffleDescriptor[inputChannels.length];
        for (ShuffleDescriptorAndIndex inputChannel : inputChannels) {
            this.shuffleDescriptors[inputChannel.getIndex()] = inputChannel.getShuffleDescriptor();
        }
    }


    public IntermediateDataSetID getResultId() {
        return resultId;
    }

    public static DownstreamTaskDeployUpdateDescriptor create(
            ScaleContext context,
            ExecutionVertex downStreamVertex,
            IntermediateResult inputResult) {

        IntermediateDataSetID dataSetID = inputResult.getId();

        ConsumedPartitionGroup consumedPartitionGroup =
                downStreamVertex.getConsumedPartitionGroupByDataSetID(dataSetID);

        final InternalExecutionGraphAccessor executionGraphAccessor =
                downStreamVertex.getExecutionGraphAccessor();

        ShuffleDescriptorAndIndex[] shuffleDescriptors =
                new ShuffleDescriptorAndIndex[consumedPartitionGroup.size()];
        int i = 0;
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            ShuffleDescriptor shuffleDescriptor =
                    TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(
                            inputResult.getPartitionById(partitionId),
                            executionGraphAccessor.getPartitionLocationConstraint(),
                            executionGraphAccessor.isNonFinishedHybridPartitionShouldBeUnknown()
                    );
            shuffleDescriptors[i] = new ShuffleDescriptorAndIndex(shuffleDescriptor,i);
            i++;
        }

        return new DownstreamTaskDeployUpdateDescriptor(
                dataSetID,
                new Tuple2<>(context.getOldParallelism(), context.getNewParallelism()),
                downStreamVertex.getExecutionVertexInputInfo(dataSetID),
                shuffleDescriptors);
    }


    // ------------------------------------------------------------------------
    public int getNumberOfNewChannels(int numOfOriginalChannels) {
        checkArgument(numOfOriginalChannels % parallelisms.f0 == 0,
                "Expected number of original channels to be a multiple of the old parallelism, " +
                        "but got %s and %s.", numOfOriginalChannels, parallelisms.f0);

        return numOfOriginalChannels / parallelisms.f0 * (parallelisms.f1 - parallelisms.f0);

    }


    public int getConsumedSubpartitionIndex() {
        return consumedSubpartitionIndex;
    }

    public NettyShuffleDescriptor getShuffleDescriptor(int i) {
        return (NettyShuffleDescriptor) shuffleDescriptors[i];
    }
}
