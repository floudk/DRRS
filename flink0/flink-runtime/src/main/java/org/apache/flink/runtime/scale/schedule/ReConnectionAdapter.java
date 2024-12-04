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

package org.apache.flink.runtime.scale.schedule;


import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.EdgeManager;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.executiongraph.VertexInputInfoStore;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.coordinator.ScaleContext;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;

import java.util.List;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;


/** Default reconnect strategy
 * Just do as what flink does.
 **/
public class ReConnectionAdapter {

    // Temporarily store the vertex input info for new connections
    private final VertexInputInfoStore tempVertexInputInfoStore;
    private final EdgeManager edgeManager;

    private final Function<IntermediateDataSetID, IntermediateResult> intermediateResultRetriever;
    private final Function<JobVertexID, ExecutionJobVertex> jobVertexRetriever;



    public ReConnectionAdapter(
            EdgeManager edgeManager,
            Function<IntermediateDataSetID, IntermediateResult> intermediateResultRetriever,
            Function<JobVertexID, ExecutionJobVertex> jobVertexRetriever) {

        this.edgeManager = edgeManager;
        this.intermediateResultRetriever = intermediateResultRetriever;
        this.jobVertexRetriever = jobVertexRetriever;
        this.tempVertexInputInfoStore = new VertexInputInfoStore();
    }

    public void reConnect(ScaleContext scaleContext, DefaultExecutionGraph executionGraph) {
        computeVertexInputInfo(scaleContext);
        ExecutionJobVertex jobVertex = scaleContext.getJobVertex();

        ExecutionVertex[] vertices = scaleContext.getTaskVertices();



        for (int i = 0; i < scaleContext.getOldParallelism(); i++) {
            final ExecutionVertex vertex = vertices[i];
            jobVertex.getInputs().forEach(
                    result -> clearCachedInformationForPartitionGroup(result, vertex)
            );
        }

        scaleContext.getDownstreamJobVertices().forEach(
                (ejv,result) -> {
                    for (ExecutionVertex vertex : ejv.getTaskVertices()) {
                        clearCachedInformationForPartitionGroup(result, vertex);
                    }
                }
        );

        rebuildConnections(
                executionGraph.getVertexInputInfoStore(),
                scaleContext);
    }

    private void clearCachedInformationForPartitionGroup(
            IntermediateResult result,
            ExecutionVertex vertex) {
        IntermediateDataSetID resultID = result.getId();
        ConsumedPartitionGroup currentConsumedPartitionGroup =
                vertex.getConsumedPartitionGroupByDataSetID(resultID);
        if( result.getCachedShuffleDescriptors(currentConsumedPartitionGroup) != null) {
            // the downstream task involved in the same currentConsumedPartitionGroup
            // may try to remove the cached shuffle descriptors multiple times.
            List<ConsumedPartitionGroup> cacheKeys = result.getShuffleDescriptorCacheKeys();
            checkArgument(
                    cacheKeys.size() == 1,
                    "Expected to have one cache key in ShuffleDescriptorCache for result "
                            + resultID + " with producer " + result.getProducer().getName()
                            + " but actually has " + cacheKeys.size() + ".");
            result.clearCachedInformationForPartitionGroup(cacheKeys.get(0));
        }
    }

    public void computeVertexInputInfo(ScaleContext scaleContext){
        ExecutionJobVertex vertex = scaleContext.getJobVertex();
        try {
            VertexInputInfoComputationUtils.computeVertexInputInfos(
                    vertex, intermediateResultRetriever).forEach(
                    (resultId, info) ->
                            this.tempVertexInputInfoStore.put(scaleContext.getJobVertexID(),
                                    resultId, info));

            for(IntermediateResult ir : vertex.getProducedDataSets()){
                for(JobVertexID jobVertexId:ir.getConsumerVertices()){
                    this.tempVertexInputInfoStore.put(
                            jobVertexId,ir.getId(),
                            VertexInputInfoComputationUtils.computeVertexInputInfoForResult(
                                    jobVertexRetriever.apply(jobVertexId), ir));
                }
            }
        } catch (JobException e) {
            throw new RuntimeException(e);
        }
    }

    public VertexInputInfoStore getTempVertexInputInfoStore() {
        return tempVertexInputInfoStore;
    }


    public void rebuildConnections(
            VertexInputInfoStore globalVertexInputInfoStore,
            ScaleContext context) {
        ExecutionJobVertex ejv = context.getJobVertex();

        // rebuildVertexToPredConnection
        List<IntermediateResult> inputs = ejv.getInputs();
        JobVertexID jobVertexID = ejv.getJobVertexId();
        edgeManager.clearVertexConsumedPartitions(ejv);
        for(IntermediateResult result:inputs){
            IntermediateDataSetID intermediateDataSetID = result.getId();
            JobVertexInputInfo newJobVertexInputInfo = tempVertexInputInfoStore.get(
                    jobVertexID,intermediateDataSetID);
            rebuildInternal(ejv, result, newJobVertexInputInfo, globalVertexInputInfoStore);
        }

        context.getDownstreamJobVertices().forEach(
                (vertex,result) -> {
                    IntermediateDataSetID intermediateDataSetID = result.getId();
                    JobVertexInputInfo newJobVertexInputInfo = tempVertexInputInfoStore.get(
                            vertex.getJobVertexId(),intermediateDataSetID);
                    edgeManager.clearVertexConsumedPartitions(vertex);
                    rebuildInternal(vertex,result,newJobVertexInputInfo,globalVertexInputInfoStore);
                }
        );
    }

    private void rebuildInternal(
            ExecutionJobVertex ejv,
            IntermediateResult result,
            JobVertexInputInfo jobVertexInputInfo,
            VertexInputInfoStore globalVertexInputInfoStore) {

        // 1. update vertexInputInfoStore
        globalVertexInputInfoStore.putOrReplace(
                ejv.getJobVertexId(),
                result.getId(),
                jobVertexInputInfo);

        // 2. update Groups in EdgeManager
        edgeManager.updateConnection(
                ejv,
                result,
                jobVertexInputInfo);
    }


}
