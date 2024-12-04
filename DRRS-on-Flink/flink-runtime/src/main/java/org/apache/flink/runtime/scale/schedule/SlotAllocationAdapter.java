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


/**
 * Use to find slots for new executions with default strategy.
 * i.e. LocalInputPreferredSlotAllocationStrategy
 * 1. find upstream with the same slot sharing group
 * 2. find any available slot
 * 3. create new slot
 */
package org.apache.flink.runtime.scale.schedule;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scale.coordinator.ScaleCoordinator;
import org.apache.flink.runtime.scheduler.ExecutionSlotAllocator;
import org.apache.flink.runtime.scheduler.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.LocalInputPreferredSlotSharingStrategy;
import org.apache.flink.runtime.scheduler.SlotSharingExecutionSlotAllocator;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;


public class SlotAllocationAdapter{

    private final Map<ConsumedPartitionGroup, LinkedHashSet<ExecutionSlotSharingGroup>>
            candidateGroupsForConsumedPartitionGroup = new IdentityHashMap<>();

    private final Set<ExecutionSlotSharingGroup> availableGroups = new LinkedHashSet<>();

    private Function<IntermediateResultPartitionID,ExecutionVertexID> executionVertexIdRetrieverByIrs;
    private Function<JobVertexID,Set<ExecutionVertexID>> executionVertexIdsRetrieverByJobVertexId;


    private final Supplier<SlotSharingGroup> onlySlotSharingGroupSupplier;
    private final Supplier<Map<ExecutionVertexID, ExecutionSlotSharingGroup>> executionSlotSharingGroupSupplier;

    private Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap = null;
    private final Function<JobVertexID, SlotSharingGroup> slotSharingGroupRetriever;

    public SlotAllocationAdapter(
            ExecutionSlotAllocator executionSlotAllocator, ScaleCoordinator coordinator){

        if(!(executionSlotAllocator instanceof SlotSharingExecutionSlotAllocator)){
            throw new UnsupportedOperationException(
                    "executionSlotAllocator must be SlotSharingExecutionSlotAllocator");
        }

        LocalInputPreferredSlotSharingStrategy slotSharingStrategy =
                ((SlotSharingExecutionSlotAllocator) executionSlotAllocator).getSlotSharingStrategy();

        this.onlySlotSharingGroupSupplier = slotSharingStrategy::getOnlySlotSharingGroup;
        this.executionSlotSharingGroupSupplier = slotSharingStrategy::getExecutionSlotSharingGroupMap;

        this.executionVertexIdRetrieverByIrs= coordinator::getExecutionVertexIdByIrs;
        this.executionVertexIdsRetrieverByJobVertexId = coordinator::getExecutionVertexIdsByJobVertexId;

        this.slotSharingGroupRetriever = coordinator::getSlotSharingGroup;
    }




    private boolean isExecutionSlotSharingGroupAvailableForVertex(
            ExecutionSlotSharingGroup executionSlotSharingGroup, ExecutionVertexID vertexId) {

        Set<JobVertexID> vertexIds = executionSlotSharingGroup.getExecutionVertexIds().stream()
                .map(ExecutionVertexID::getJobVertexId).collect(Collectors.toSet());

        // if not include the jobVertexId, it is available
        return !vertexIds.contains(vertexId.getJobVertexId());
    }


    public void findSlotsForNewExecutions(List<SchedulingExecutionVertex> newVertices){


        executionSlotSharingGroupMap = executionSlotSharingGroupSupplier.get();

        for (SchedulingExecutionVertex executionVertex :newVertices){

//            ExecutionSlotSharingGroup group =
//                    tryFindAvailableProducerExecutionSlotSharingGroupFor(executionVertex);
//
//            if(group==null){
//                group = tryFindAvailableExecutionSlotSharingGroupFor(executionVertex);
//            }
//
//            if(group==null){
//               group = createNewExecutionSlotSharingGroup(executionVertex);
//            }
            // always create new group

            ExecutionSlotSharingGroup group = createNewExecutionSlotSharingGroup(executionVertex.getId());

            addVertexToExecutionSlotSharingGroup(executionVertex.getId(), group);
        }
    }

    private void addVertexToExecutionSlotSharingGroup(
            ExecutionVertexID executionVertexId, ExecutionSlotSharingGroup group) {
        group.addVertex(executionVertexId);
        executionSlotSharingGroupMap.put(executionVertexId, group);

        availableGroups.remove(group);

    }

    private ExecutionSlotSharingGroup createNewExecutionSlotSharingGroup(
            ExecutionVertexID executionVertexID) {

        final SlotSharingGroup slotSharingGroup =
                slotSharingGroupRetriever.apply(executionVertexID.getJobVertexId());
        checkNotNull(slotSharingGroup, "slotSharingGroup must not be null");

        final ExecutionSlotSharingGroup newGroup = new ExecutionSlotSharingGroup();

        newGroup.setResourceProfile(slotSharingGroup.getResourceProfile());
        return newGroup;
    }


    protected ExecutionSlotSharingGroup tryFindAvailableProducerExecutionSlotSharingGroupFor(
            SchedulingExecutionVertex executionVertex){
        final ExecutionVertexID executionVertexId = executionVertex.getId();
        for (ConsumedPartitionGroup consumedPartitionGroup :
                executionVertex.getConsumedPartitionGroups()){

            Set<ExecutionSlotSharingGroup> candidateGroups =
                    candidateGroupsForConsumedPartitionGroup.computeIfAbsent(
                            consumedPartitionGroup,
                            this::computeCandidateGroupsForConsumedPartitionGroup);

            Iterator<ExecutionSlotSharingGroup> candidateIterator = candidateGroups.iterator();

            while (candidateIterator.hasNext()) {
                ExecutionSlotSharingGroup candidateGroup = candidateIterator.next();
                candidateIterator.remove();
                if (isExecutionSlotSharingGroupAvailableForVertex(
                        candidateGroup, executionVertexId)) {
                    return candidateGroup;
                }
            }
        }
        return null;
    }


    private ExecutionSlotSharingGroup tryFindAvailableExecutionSlotSharingGroupFor(
            SchedulingExecutionVertex executionVertex) {

        computeAvailableGroupsForJobVertex(executionVertex.getId().getJobVertexId());

        if(!availableGroups.isEmpty()){
            return availableGroups.iterator().next();
        }
        return null;
    }

    private void computeAvailableGroupsForJobVertex(JobVertexID jobVertexID) {

        for (ExecutionSlotSharingGroup executionSlotSharingGroup : executionSlotSharingGroupMap.values()) {
            Set<ExecutionVertexID> executionVertexIds = executionVertexIdsRetrieverByJobVertexId.apply(jobVertexID);
            // if executionSlotSharingGroup doesn't contain any executionVertexId, it is available
            if (executionSlotSharingGroup.disjoint(executionVertexIds)) {
                availableGroups.add(executionSlotSharingGroup);
            }
        }
    }

    private LinkedHashSet<ExecutionSlotSharingGroup> computeCandidateGroupsForConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {

        final LinkedHashSet<ExecutionSlotSharingGroup> candidateExecutionSlotSharingGroups =
                new LinkedHashSet<>();

        for (IntermediateResultPartitionID consumedPartition : consumedPartitionGroup) {

            ExecutionVertexID producerExecutionVertexId =
                    executionVertexIdRetrieverByIrs.apply(consumedPartition);

            ExecutionSlotSharingGroup assignedGroupForProducerExecutionVertex =
                    executionSlotSharingGroupMap.get(producerExecutionVertexId);


            checkNotNull(assignedGroupForProducerExecutionVertex);

            candidateExecutionSlotSharingGroups.add(
                    assignedGroupForProducerExecutionVertex);
        }
        return candidateExecutionSlotSharingGroups;
    }

}
