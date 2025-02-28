package org.apache.flink.runtime.scale.coordinator;

import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.scale.schedule.ReConnectionAdapter;
import org.apache.flink.runtime.scale.schedule.SlotAllocationAdapter;
import org.apache.flink.runtime.scale.schedule.StateRepartitionAdapter;
import org.apache.flink.runtime.scale.io.message.TaskScaleDescriptor;
import org.apache.flink.runtime.scale.io.message.deploy.DownstreamTaskDeployUpdateDescriptor;
import org.apache.flink.runtime.scheduler.DefaultExecutionDeployer;
import org.apache.flink.runtime.scheduler.ExecutionSlotAllocator;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class is responsible for:
 * 1. Upon receiving a scale request, it will check the validity and calculate the new key-partition assignment
 * 2. Update Logic Objects, including ExecutionJobVertex, ExecutionVertex,etc.
 * 3. Notify all tasks to trigger scale
 * 4. Choose next subscale to trigger until all key-partition assignment is updated
 * 5.
 */

public class ScaleCoordinator{

    private static final Logger LOG = LoggerFactory.getLogger(ScaleCoordinator.class);

    // Logic Objects
    private final DefaultExecutionGraph executionGraph;
    private final DefaultExecutionTopology executionTopology;


    // Adapters
    private final SlotAllocationAdapter slotAllocationAdapter;
    private final ReConnectionAdapter reConnectionAdapter;
    private final StateRepartitionAdapter stateRepartitionAdapter;

    // Supp objects
    private final Map<String, JobVertexID> jobVertexNameToIDMap;

    private final DefaultExecutionDeployer executionDeployer;
    private final ExecutionVertexVersioner executionVertexVersioner;

    private final Map<TriggerId, ScaleHandler> subscaleHandlers = new HashMap<>();
    private final Map<JobVertexID, TriggerId> vertexIDTriggerID = new HashMap<>();

    private List<Execution> executionToTrigger;


    public ScaleCoordinator(ExecutionGraph deg,
                            ExecutionSlotAllocator slotAllocator,
                            DefaultExecutionDeployer executionDeployer,
                            ExecutionVertexVersioner executionVertexVersioner) {

        this.executionGraph = (DefaultExecutionGraph) deg;
        this.executionTopology = (DefaultExecutionTopology) executionGraph.getSchedulingTopology();

        this.slotAllocationAdapter = new SlotAllocationAdapter(slotAllocator, this);
        this.reConnectionAdapter = new ReConnectionAdapter(
                ((DefaultExecutionGraph) deg).getEdgeManager(),
                deg.getAllIntermediateResults()::get,
                deg::getJobVertex);
        this.stateRepartitionAdapter = new StateRepartitionAdapter.DefaultStateRepartitionAdapter();

        // initialize supp
        this.jobVertexNameToIDMap = new HashMap<>();
        for (JobVertexID jobVertexID : executionGraph.getAllVertices().keySet()) {
            JobVertex jobVertex = executionGraph.getJobVertex(jobVertexID).getJobVertex();
            jobVertexNameToIDMap.put(jobVertex.getName(), jobVertexID);
        }

        this.executionDeployer = executionDeployer;
        this.executionVertexVersioner = executionVertexVersioner;
    }

    // -------------------------- register new scale --------------------------
    public CompletableFuture<Void> triggerScale(TriggerId triggerID, String operatorName, int newParallelism) {

        JobVertexID jobVertexID = jobVertexNameToIDMap.get(operatorName);
        checkNotNull(jobVertexID, "Failed to find JobVertexID by operatorName: " + operatorName);

        // check if the scale is valid
        ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);

        int oldParallelism = executionJobVertex.getParallelism();
        int maxParallelism = executionJobVertex.getMaxParallelism();
        if (newParallelism <= 0 ||
            newParallelism == oldParallelism ||
            newParallelism > maxParallelism) {
            LOG.error("Invalid scale request for jobVertex {} from {} to {} (max: {})",
                    operatorName, oldParallelism, newParallelism, maxParallelism);
            return FutureUtils.completedExceptionally(
                    new IllegalArgumentException("Invalid scale request"));
        }

        LOG.info("Triggering scale for operator {} from {} to {} with meces",
                operatorName, oldParallelism, newParallelism);

        CompletableFuture<Void> scaleCompleteFuture = new CompletableFuture<>();

        if (executionToTrigger == null) {
            executionToTrigger = executionGraph.getExecutionsToTrigger();
        }

        try {
            ScaleContext context = new ScaleContext(executionJobVertex, newParallelism, executionToTrigger);
            expandDeployment(context)
                    .thenRun(()-> triggerScaleInternal(triggerID, context, scaleCompleteFuture));
        }catch (Exception e){
            LOG.error("Failed to scale jobVertex {}", executionJobVertex.getName(), e);
            scaleCompleteFuture.completeExceptionally(e);
        }

        return scaleCompleteFuture;
    }

    public String getScaleStatus(TriggerId triggerId) {
        if (subscaleHandlers.containsKey(triggerId)) {
            return "SUCCESS";
        } else {
            return "IN_PROGRESS";
        }
    }

    private void triggerScaleInternal(
            TriggerId triggerID,
            ScaleContext scaleContext,
            CompletableFuture<Void> scaleCompleteFuture) {


        // CHECK: is it necessary to use the thread from RPC as the main thread of scale coordinator
        LOG.info("JobVertex {} start scaling({} -> {}) in Thread {}",
                scaleContext.getJobVertexName(),
                scaleContext.getOldParallelism(),
                scaleContext.getNewParallelism(),
                Thread.currentThread().getName());


        // 1. calculate the new key-partition (scale decision)
        stateRepartitionAdapter.calculateNewKeyPartitions(scaleContext);

        // 2. notify all tasks to reset subtask scale coordinator
        ExecutionVertex[] taskVertices = scaleContext.getTaskVertices();
        List<CompletableFuture<Void>> resetFutures = new ArrayList<>();
        for (ExecutionVertex taskVertex : taskVertices) {
            LOG.info(
                    "Coordinator: Resetting subtask scale coordinator for task {}",
                    taskVertex.getTaskNameWithSubtaskIndex());
            try {
                resetFutures.add(
                        taskVertex.getCurrentExecutionAttempt()
                                .resetSubtaskScaleCoordinator(new TaskScaleDescriptor(scaleContext)));
            } catch (Exception e) {
                LOG.error("Failed to create TaskScaleDescriptor for task {}",
                        taskVertex.getTaskNameWithSubtaskIndex(), e);
            }
        }
        scaleContext.resetFuture = FutureUtils.waitForAll(resetFutures);

        // 3. ready to receive subscale requests
        ScaleHandler scaleHandler = new ScaleHandler(scaleContext,scaleCompleteFuture);
        if (subscaleHandlers.put(triggerID, scaleHandler) != null) {
            LOG.warn("Replaced existing SubscaleHandler for jobVertex {}",
                    scaleContext.getJobVertexName());
        }
        vertexIDTriggerID.put(scaleContext.getJobVertexID(), triggerID);

        scaleHandler.scaleCompleteFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                LOG.error("Failed to scale jobVertex {}", scaleContext.getJobVertexName(), throwable);
            } else {
                reduceDeployment(scaleContext);
            }
        });

        // trigger all subscales and end the scale
        List<Integer> involvedKeyGroups = scaleContext.getAllMigratedKeyGroups();
        scaleHandler.trigger(involvedKeyGroups);
    }

    private CompletableFuture<Void> expandDeployment(ScaleContext scaleContext){
        if (!scaleContext.isScaleOut()){
            // no need to expand in scale-in
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // 0. necessary scheduling updates
        Arrays.stream(scaleContext.getJobVertex().getProducedDataSets()).forEach(
                (intermediateDataSet) ->
                        intermediateDataSet.resizePartitions(scaleContext.getNewParallelism())
        );
        scaleContext.getJobVertex().addExecutionVertices(scaleContext.getNewParallelism());

        List<ExecutionVertex> newExecutions = Arrays.stream(
                scaleContext.getJobVertex().getTaskVertices(),
                        scaleContext.getOldParallelism(),
                        scaleContext.getNewParallelism()).collect(Collectors.toList());
        executionGraph.expandGraph(newExecutions);
        reConnectionAdapter.reConnect(scaleContext, executionGraph);

        executionTopology.expandTopology(scaleContext, slotAllocationAdapter);

        LOG.info("Schedule updates for scaling jobVertex {} has been done in Thread {}",
                scaleContext.getJobVertexName(), Thread.currentThread().getName());

        // 1. update upstream operators for new downstream partitions
        scaleContext.getUpstreamJobVertices().forEach(
                (jobVertex, intermediateResult) -> {
                    IntermediateResultPartition[]  partitions = intermediateResult.getPartitions();
                    ExecutionVertex[] taskVertices = jobVertex.getTaskVertices();
                    for (int id=0; id< taskVertices.length; id++){
                        CompletableFuture<Void> future =
                                taskVertices[id].getCurrentExecutionAttempt().updateUpstreamResultPartitions(
                                        partitions[id].getPartitionId(), scaleContext.getNewParallelism());
                        futures.add(future);
                    }
                }
        );
        LOG.info("Upstream operators for scaling jobVertex {} have been updated in Thread {}",
                scaleContext.getJobVertexName(), Thread.currentThread().getName());

        // 2. deploy new tasks
        for (ExecutionVertex executionVertex : newExecutions) {
            CompletableFuture<Void> deployFuture = new CompletableFuture<>();
            executionVertex.newExecutionDeployedFuture = deployFuture;
            futures.add(deployFuture);

            executionVertex.newExecutionRunningFuture = new CompletableFuture<>();
        }

        executionDeployer.allocateSlotsAndDeploy(
                newExecutions.stream().map(ExecutionVertex::getCurrentExecutionAttempt).collect(Collectors.toList()),
                executionVertexVersioner.recordVertexModifications(newExecutions.stream()
                        .map(ExecutionVertex::getID)
                        .collect(Collectors.toList())));

        CompletableFuture<Void> allDeployedFuture = FutureUtils.waitForAll(futures);

        LOG.info("New tasks for scaling jobVertex {} have been deployed in Thread {}",
                scaleContext.getJobVertexName(), Thread.currentThread().getName());



        // 3. update downstream operators for new upstream inputs
        scaleContext.getDownstreamJobVertices().forEach((jobVertex, intermediateResult) -> {
            ExecutionVertex[] taskVertices = jobVertex.getTaskVertices();
                for (ExecutionVertex taskVertex : taskVertices) {
                    CompletableFuture<Void> future =
                            allDeployedFuture.thenCompose((ignored) ->
                                    taskVertex.getCurrentExecutionAttempt().updateDownstreamInputgates(
                                            DownstreamTaskDeployUpdateDescriptor
                                                    .create(scaleContext, taskVertex, intermediateResult)));

                    futures.add(future);
                }
            }
        );
        LOG.info("Downstream operators for scaling jobVertex {} have been updated in Thread {}",
                scaleContext.getJobVertexName(), Thread.currentThread().getName());

        return FutureUtils.waitForAll(futures);
    }

    private void reduceDeployment(ScaleContext scaleContext){
        // TODO: remove useless tasks(scale-in) and release scale resources if necessary
        LOG.info("Start reducing deployment for jobVertex {} in Thread {}",
                scaleContext.getJobVertexName(), Thread.currentThread().getName());
    }


    public SlotSharingGroup getSlotSharingGroup(JobVertexID jobVertexID){
        return executionGraph.getJobVertex(jobVertexID).getSlotSharingGroup();
    }

    // ------------------------------------------------------
    // Termination stage: Future Utils
    // ------------------------------------------------------


    public ExecutionVertexID getExecutionVertexIdByIrs(IntermediateResultPartitionID partitionId){
        return executionGraph.getResultPartitionOrThrow(partitionId).getProducer().getID();
    }
    public Set<ExecutionVertexID> getExecutionVertexIdsByJobVertexId(JobVertexID jobVertexID) {
        return Stream.of(executionGraph.getJobVertex(jobVertexID).getTaskVertices())
                .map(ExecutionVertex::getID)
                .collect(Collectors.toSet());
    }


    public boolean acknowledgeScaleComplete(ExecutionAttemptID executionAttemptID) {
        LOG.info("Acknowledging scale complete for task {}", executionAttemptID.getExecutionVertexId());
        final ExecutionVertex vertex = executionGraph.getExecutionVertexOrThrow(executionAttemptID.getExecutionVertexId());
        final ScaleHandler handler = subscaleHandlers.get(vertexIDTriggerID.get(vertex.getJobvertexId()));
        if (handler == null) {
            LOG.error("No SubscaleHandler for jobVertex {}", vertex.getJobvertexId());
            throw new IllegalStateException("No SubscaleHandler for jobVertex " + vertex.getJobvertexId());
        }
        handler.acknowledgeSubscaleComplete(executionAttemptID.getSubtaskIndex());
        return true;
    }
}
