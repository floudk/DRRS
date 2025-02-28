package org.apache.flink.runtime.scale;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.runtime.scale.state.FlexibleKeyGroupRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ScalingContext {
    static final Logger LOG = LoggerFactory.getLogger(ScalingContext.class);

    private final TaskInfo taskInfo;

    private final int[] parallelismPair; // [oldParallelism, newParallelism]
    // its real holder: keyContext
    private FlexibleKeyGroupRange localKeyGroups;

    // key groups from other tasks that are sent to this task

    private final Set<Integer> pendingInKeys = new HashSet<>(); // updated by main thread immediately after states merged

    // key groups from this task that are sent to other tasks
    private final Map<Integer, List<Integer>> targetTaskWithOutKeys = new HashMap<>();
    private final Map<Integer, Integer> pendingOutKeys = new HashMap<>(); // keys ready to be sent out

    private final Map<Set<Integer>, Integer> subscaleIDMap = new HashMap<>();
    private int subscaleID = -1;

    private CompletableFuture<Void> completeFuture;

    private final SubscaleTracker subscaleTracker;

    public ScalingContext(
            TaskInfo taskInfo,
            Consumer<Set<Integer>> subscaleTrackerNotifier) {
        this.taskInfo = taskInfo;
        // initialize parallelismPair to [-1, -1]
        this.parallelismPair = new int[]{-1, -1};
        subscaleTracker = new SubscaleTracker(subscaleTrackerNotifier);
    }


    public void updateTaskInfo(int newParallelism) {
        parallelismPair[0] = taskInfo.getNumberOfParallelSubtasks();
        parallelismPair[1] = newParallelism;
        LOG.info("{} update task info with parallelisms: {} -> {}", taskInfo.getTaskNameWithSubtasks(), parallelismPair[0],  parallelismPair[1]);
        taskInfo.scale(newParallelism);
    }


    public void triggered(FlexibleKeyGroupRange currentKeyGroups){
        transition(Stage.TRIGGERED);
        this.localKeyGroups = currentKeyGroups;
        if (isNewlyCreatedTask()) {
            this.localKeyGroups.clear();
        }
    }

    // --------------------------- Key Group Tracking ---------------------------

    /**
     *
     * @param sourceTaskWithInKeys: key groups from other tasks that are sent to this task
     * @param targetTaskWithOutKeys: key groups from this task that are sent to other tasks
     */
    public void subscale(Map<Integer, List<Integer>> sourceTaskWithInKeys,
                         Map<Integer, List<Integer>> targetTaskWithOutKeys) {
        LOG.info("{} set subscale context with sourceTaskWithInKeys: {} and targetTaskWithOutKeys: {}, current local key groups: {}",
                taskInfo.getTaskNameWithSubtasks(), sourceTaskWithInKeys, targetTaskWithOutKeys, localKeyGroups);

        subscaleID++;

        for (List<Integer> keys : sourceTaskWithInKeys.values()) {
            pendingInKeys.addAll(keys);
        }

        this.targetTaskWithOutKeys.putAll(targetTaskWithOutKeys);

        Set<Integer> outKeysInSubscale = new HashSet<>();
        for (Map.Entry<Integer, List<Integer>> entry : targetTaskWithOutKeys.entrySet()) {
            for (int keyGroup : entry.getValue()) {
                pendingOutKeys.put(keyGroup, entry.getKey());
            }
            outKeysInSubscale.addAll(entry.getValue());
        }
        subscaleIDMap.put(Collections.unmodifiableSet(outKeysInSubscale), subscaleID);
        subscaleTracker.addSubscale(sourceTaskWithInKeys);
    }

    public boolean isLocalKeyGroup(int keyGroupIndex) {
        return localKeyGroups.contains(keyGroupIndex);
    }

    // return true if the key group is
    // 1. in pendingInKeys: still waiting for the key group to be received
    // 2. in completedInKeys: already received the key group but do not get all remote confirmations
    public boolean isIncomingKey(int keyGroupIndex) {
        return pendingInKeys.contains(keyGroupIndex);
    }
    public int getTargetTask(int keyGroupIndex) {
        if (pendingOutKeys.containsKey(keyGroupIndex)) {
            return pendingOutKeys.get(keyGroupIndex);
        }
        LOG.error("Current targetTaskWithOutKeys: {}", targetTaskWithOutKeys);
        LOG.error("Current pendingOutKeys: {}", pendingOutKeys);
        throw new RuntimeException("Key group " + keyGroupIndex + " is not in any target task");
    }

    public boolean isNewlyCreatedTask() {
        checkArgument(parallelismPair[0] != -1 && parallelismPair[1] != -1,
                "parallelismPair is not initialized");
        // only newly created task has parallelismPair[0] == parallelismPair[1]
        // since when the task is created, the parallelism has already been set to the new value
        return parallelismPair[0] == parallelismPair[1];
    }

    // must running in main thread
    public void removeFromOutPendingKeys(List<Integer> keyGroups) {
        keyGroups.forEach(
                keyGroup -> {
                    checkNotNull(pendingOutKeys.remove(keyGroup),
                            "Key group " + keyGroup + " is not in pendingOutKeys");
                }
        );
        if (pendingOutKeys.isEmpty() && status == Stage.COMPLETING && checkAllSubscaleComplete()) {
            LOG.info("{} all subtasks have completed, notify the coordinator", taskInfo.getTaskNameWithSubtasks());
            transition(Stage.NON_SCALE);
            completeFuture.complete(null);
        }
    }


    public void removeFromInPendingKeys(Set<Integer> ingoingKeyGroupRange) {
        for (int keyGroup : ingoingKeyGroupRange) {
            pendingInKeys.remove(keyGroup);
            subscaleTracker.notifyMigratedIn(keyGroup);
        }
        if (pendingInKeys.isEmpty() && status == Stage.COMPLETING && checkAllSubscaleComplete()) {
            LOG.info("{} all subtasks have completed, notify the coordinator", taskInfo.getTaskNameWithSubtasks());
            transition(Stage.NON_SCALE);
            completeFuture.complete(null);
        }
    }

    public Set<Integer> getOutInvolvedTasks(Set<Integer> involvedKeys) {
        Set<Integer> relatedTasks = new HashSet<>();
        involvedKeys.forEach(
                keyGroup -> {
                    if (pendingOutKeys.containsKey(keyGroup)) {
                        relatedTasks.add(pendingOutKeys.get(keyGroup));
                    }
                }
        );
        return relatedTasks;
    }

    public int getOutKeySubscaleID(int keyGroup) {
        for (Map.Entry<Set<Integer>, Integer> entry : subscaleIDMap.entrySet()) {
            if (entry.getKey().contains(keyGroup)) {
                return entry.getValue();
            }
        }
        LOG.error("Current subscaleIDMap: {}", subscaleIDMap);
        throw new RuntimeException("Key group " + keyGroup + " is not in any subscale");
    }

    public int getOutKeySubscaleIDBySet(Set<Integer> keyGroups) {
        for (Map.Entry<Set<Integer>, Integer> entry : subscaleIDMap.entrySet()) {
            // if intersection is not empty, return the subscaleID
            if (!Collections.disjoint(entry.getKey(), keyGroups)) {
                return entry.getValue();
            }
        }
        LOG.error("Current subscaleIDMap: {}", subscaleIDMap);
        throw new RuntimeException("Key groups " + keyGroups + " is not in any subscale");
    }

    public void registerCompleteNotifier(Runnable completeNotifier) {
        transition(Stage.COMPLETING);
        // check if all subtasks have completed, if so, notify the coordinator
        // if not, wait for the completion of all subtasks

        // check out keys
        if (checkAllSubscaleComplete()) {
            // notify the coordinator
            LOG.info("{} all subtasks have completed, notify the coordinator", taskInfo.getTaskNameWithSubtasks());
            transition(Stage.NON_SCALE); // maybe should run in main thread
            completeNotifier.run();
        } else {
            // wait for the completion of all subtasks
            completeFuture = new CompletableFuture<>();
            completeFuture.thenRun(completeNotifier);
        }
    }

    private boolean checkAllSubscaleComplete() {
        boolean completed = pendingOutKeys.isEmpty() && pendingInKeys.isEmpty();
        if (!completed) {
            LOG.info(
                    "{} checkComplete failed, pendingOutKeys: {}, pendingInKeys: {}",
                    taskInfo.getTaskNameWithSubtasks(),
                    pendingOutKeys,
                    pendingInKeys);
        }
        return completed;
    }


    // --------------------------- Status Tracking ---------------------------

    public enum Stage {
        NON_SCALE,
        TRIGGERED,
        COMPLETING,
    }
    private Stage status = Stage.NON_SCALE; // default status

    // synchronized to avoid concurrent status change
    private synchronized void transition(Stage next) {
        Stage oldStatus = status;
        status = next;
        LOG.info("{} status transition from {} to {}", taskInfo.getTaskNameWithSubtasks(), oldStatus, status);
    }

    @Override
    public String toString() {
        return status.toString();
    }
    public boolean isScaling() {
        return status != Stage.NON_SCALE;
    }
    public String getTaskName() {
        return taskInfo.getTaskNameWithSubtasks();
    }
    public int getSubtaskIndex() {
        return taskInfo.getIndexOfThisSubtask();
    }

}
