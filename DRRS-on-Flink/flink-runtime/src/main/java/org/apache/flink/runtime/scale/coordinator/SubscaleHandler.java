package org.apache.flink.runtime.scale.coordinator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scale.io.SubscaleTriggerInfo;
import org.apache.flink.runtime.scale.state.FlexibleKeyGroupRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class SubscaleHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SubscaleHandler.class);

    private final List<Execution> upstreamExecutions = new ArrayList<>();


    // key group -> (original partition, target partition)
    private final List<Tuple2<Integer,Integer>> positionedKeyGroups = new ArrayList<>();

    private final CompletableFuture<Void> readyForSubscaleFuture;

    public final CompletableFuture<Void> scaleCompleteFuture;
    private final Boolean[] scaleCompleteAcknowledgement;

    AtomicInteger subscaleIDRetriever  = new AtomicInteger(0);

    public final ScaleContext scaleContext;

    public SubscaleHandler(ScaleContext scaleContext, CompletableFuture<Void> scaleCompleteFuture) {
        this.scaleContext = scaleContext;

        for (ExecutionJobVertex upstreamJobVertex : scaleContext.getUpstreamJobVertices().keySet()) {
            for (ExecutionVertex vertex : upstreamJobVertex.getTaskVertices()) {
                upstreamExecutions.add(vertex.getCurrentExecutionAttempt());
            }
        }

        List<FlexibleKeyGroupRange> aimedPartitioning = scaleContext.getNewKeyGroupPartitions();
        List<FlexibleKeyGroupRange> currentPartitioning = scaleContext.getOldKeyGroupPartitions();
        int maxKeyGroup = scaleContext.getMaxParallelism();
        for (int i = 0; i < maxKeyGroup; i++) {
            positionedKeyGroups.add(new Tuple2<>(-1, -1));
        }
        for (int i = 0; i < currentPartitioning.size(); i++) {
            for (int keyGroup : currentPartitioning.get(i)){
                positionedKeyGroups.get(keyGroup).f0 = i;
            }
        }
        for (int i = 0; i < aimedPartitioning.size(); i++) {
            for (int keyGroup : aimedPartitioning.get(i)){
                positionedKeyGroups.get(keyGroup).f1 = i;
            }
        }
        this.readyForSubscaleFuture = checkNotNull(scaleContext.resetFuture);
        this.scaleCompleteFuture = scaleCompleteFuture;

        int ackNum = Math.max(scaleContext.getNewParallelism(), scaleContext.getOldParallelism());
        scaleCompleteAcknowledgement = new Boolean[ackNum];
        Arrays.fill(scaleCompleteAcknowledgement, false);
    }

    public void trigger(List<Integer> involvedKeyGroups, Map<Integer, Long> keyGroupStateSize) {
        LOG.info("Triggering subscale with involved key groups: {}", involvedKeyGroups);
        Map<Integer, SubscaleTriggerInfo> info = new HashMap<>();

        // check all key groups: position.f0 != position.f1
        involvedKeyGroups.forEach(
                kg -> {
                    Tuple2<Integer, Integer> position = positionedKeyGroups.get(kg);
                    checkState(!position.f0.equals(position.f1),
                            "Key group " + kg + " has already been moved to the target partition: " + positionedKeyGroups);
                    info.put(kg, new SubscaleTriggerInfo(position.f1, keyGroupStateSize.get(kg)));
                }
        );

        // trigger subscale after readyForSubscaleFuture is completed
        int subscaleID = subscaleIDRetriever.getAndIncrement();
        readyForSubscaleFuture.thenRun(() -> upstreamExecutions.forEach(
                execution -> execution.triggerSubscale(info,subscaleID)
        ));
    }

    public void finishSubscales() {
        LOG.info("Finishing subscales.");
        readyForSubscaleFuture.thenRun(() -> {
            upstreamExecutions.get(0).finishSubscale();
        });
    }

    public void acknowledgeAllSubscaleComplete(int subtaskIndex) {
        checkArgument(subtaskIndex >= 0 && subtaskIndex < scaleCompleteAcknowledgement.length,
                "Invalid subtask index: " + subtaskIndex);
        checkArgument(!scaleCompleteAcknowledgement[subtaskIndex],
                "Subtask " + subtaskIndex + " has already acknowledged subscale completion.");

        scaleCompleteAcknowledgement[subtaskIndex] = true;
        if (Arrays.stream(scaleCompleteAcknowledgement).allMatch(Boolean::booleanValue)) {
            LOG.info("All subtasks have acknowledged subscale completion.");
            scaleCompleteFuture.complete(null);
        }
    }
}
