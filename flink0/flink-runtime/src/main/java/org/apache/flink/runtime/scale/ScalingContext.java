package org.apache.flink.runtime.scale;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.runtime.scale.state.FlexibleKeyGroupRange;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;

public class ScalingContext {
    static final Logger LOG = LoggerFactory.getLogger(ScalingContext.class);

    private final TaskInfo taskInfo;

    private final int[] parallelismPair; // [oldParallelism, newParallelism]
    // its real holder: keyContext
    private FlexibleKeyGroupRange localKeyGroups;

    // key groups from other tasks that are sent to this task

    private final Set<Integer> pendingInKeys = new HashSet<>(); // updated by main thread immediately after states merged
    private final Set<Integer> allConfirmedInKeys = new HashSet<>(); // keys that have all confirmed but not received
    private final Set<Integer> completedInKeys = new HashSet<>(); // updated by main thread immediately after states merged

    // key groups from this task that are sent to other tasks
    private final Map<Integer, List<Integer>> targetTaskWithOutKeys = new HashMap<>();
    private final Map<Integer, Integer> pendingOutKeys = new HashMap<>(); // keys ready to be sent out
    private final Set<Integer> allConfirmedOutKeys = new HashSet<>(); // keys that have all confirmed but may not send out
    private final Map<Integer, Integer> completedOutKeys = new HashMap<>(); //keys already sent out but not all confirmations rerouted


    private CompletableFuture<Void> completeFuture;

    private final SubscaleTracker subscaleTracker;
    public KeyGroupRange  finalKeyGroupRange;


    private final Map<Integer, Integer> keyToSubscaleID = new HashMap<>();

    public ScalingContext(
            TaskInfo taskInfo,
            Consumer<Set<Integer>> subscaleTrackerNotifier,
            Consumer<Integer> channelCloser) {
        this.taskInfo = taskInfo;
        // initialize parallelismPair to [-1, -1]
        this.parallelismPair = new int[]{-1, -1};
        subscaleTracker = new SubscaleTracker(subscaleTrackerNotifier, channelCloser);
    }


    public void updateTaskInfo(int newParallelism) {
        parallelismPair[0] = taskInfo.getNumberOfParallelSubtasks();
        parallelismPair[1] = newParallelism;
        LOG.info("{} update task info with parallelisms: {} -> {}", taskInfo.getTaskNameWithSubtasks(), parallelismPair[0],  parallelismPair[1]);
        taskInfo.scale(newParallelism);
        this.finalKeyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                taskInfo.getMaxNumberOfParallelSubtasks(),
                newParallelism,
                taskInfo.getIndexOfThisSubtask());
    }


    public void triggered(FlexibleKeyGroupRange currentKeyGroups){
        transition(Stage.TRIGGERED);
        this.localKeyGroups = currentKeyGroups;
        if (isNewlyCreatedTask()) {
            this.localKeyGroups.clear();
        }
    }

    public int getSubscaleID(int keyGroupIndex) {
        checkArgument(keyToSubscaleID.containsKey(keyGroupIndex),
                "keyGroupIndex %s is not in keyToSubscaleID %s", keyGroupIndex, keyToSubscaleID);
        return keyToSubscaleID.get(keyGroupIndex);
    }


    // --------------------------- Key Group Tracking ---------------------------

    /**
     *
     * @param sourceTaskWithInKeys: key groups from other tasks that are sent to this task
     * @param targetTaskWithOutKeys: key groups from this task that are sent to other tasks
     */
    public void subscale(Map<Integer, List<Integer>> sourceTaskWithInKeys,
                         Map<Integer, List<Integer>> targetTaskWithOutKeys,
                        int subscaleID) {



        for (List<Integer> keys : sourceTaskWithInKeys.values()) {
            pendingInKeys.addAll(keys);
            keys.forEach(
                    keyGroup -> keyToSubscaleID.put(keyGroup, subscaleID)
            );
        }

        this.targetTaskWithOutKeys.putAll(targetTaskWithOutKeys);

        for (Map.Entry<Integer, List<Integer>> entry : targetTaskWithOutKeys.entrySet()) {
            for (int keyGroup : entry.getValue()) {
                pendingOutKeys.put(keyGroup, entry.getKey());
                keyToSubscaleID.put(keyGroup, subscaleID);
            }
        }
        subscaleTracker.addSubscale(sourceTaskWithInKeys, subscaleID);

        LOG.info("{}: set subscale context with sourceTaskWithInKeys: {} and targetTaskWithOutKeys: {}, current local key groups: {}, keyToSubscaleID: {}",
                subscaleID, sourceTaskWithInKeys, targetTaskWithOutKeys, localKeyGroups, keyToSubscaleID);
    }

    public boolean isLocalKeyGroup(int keyGroupIndex) {
        return localKeyGroups.contains(keyGroupIndex);
    }

    // return true if the key group is
    // 1. in pendingInKeys: still waiting for the key group to be received
    // 2. in completedInKeys: already received the key group but do not get all remote confirmations
    public boolean isIncomingKey(int keyGroupIndex) {
        return pendingInKeys.contains(keyGroupIndex) || completedInKeys.contains(keyGroupIndex);
    }
    public int getTargetTask(int keyGroupIndex) {
        if (pendingOutKeys.containsKey(keyGroupIndex)) {
            return pendingOutKeys.get(keyGroupIndex);
        } else if (completedOutKeys.containsKey(keyGroupIndex)) {
            return completedOutKeys.get(keyGroupIndex);
        }
        LOG.error("Current targetTaskWithOutKeys: {}", targetTaskWithOutKeys);
        LOG.error("Current pendingOutKeys: {}", pendingOutKeys);
        LOG.error("Current completedOutKeys: {}", completedOutKeys);
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
                    int targetTask = pendingOutKeys.remove(keyGroup);
                    if (!allConfirmedOutKeys.contains(keyGroup)) {
                        completedOutKeys.put(keyGroup, targetTask);
                    }
                }
        );
        if (pendingOutKeys.isEmpty() && status == Stage.COMPLETING && checkAllSubscaleComplete()) {
            LOG.info("{} all subtasks have completed, notify the coordinator", taskInfo.getTaskNameWithSubtasks());
            transition(Stage.NON_SCALE);
            completeFuture.complete(null);
        }
    }
    public void notifyAllConfirmBarriersRerouted(Set<Integer> involvedKeys){
        involvedKeys.forEach(
                keyGroup -> {
                    if (completedOutKeys.containsKey(keyGroup)) {
                        completedOutKeys.remove(keyGroup);
                    } else {
                        allConfirmedOutKeys.add(keyGroup);
                    }
                }
        );
        if (completedOutKeys.isEmpty() && status == Stage.COMPLETING && checkAllSubscaleComplete()) {
            LOG.info("{} all subtasks have completed, notify the coordinator", taskInfo.getTaskNameWithSubtasks());
            transition(Stage.NON_SCALE);
            completeFuture.complete(null);
        }
    }

    public void removeFromInPendingKeys(Set<Integer> ingoingKeyGroupRange, int fromTaskIndex) {
        for (int keyGroup : ingoingKeyGroupRange) {
            pendingInKeys.remove(keyGroup);
            if (!allConfirmedInKeys.contains(keyGroup)) {
                completedInKeys.add(keyGroup);
            }else{
                subscaleTracker.notifyMigratedIn(keyGroup, keyToSubscaleID.get(keyGroup), fromTaskIndex);
            }
            subscaleTracker.notifyTransferredIn(keyGroup, keyToSubscaleID.get(keyGroup), fromTaskIndex);
        }
        if (pendingInKeys.isEmpty() && status == Stage.COMPLETING && checkAllSubscaleComplete()) {
            LOG.info("{} all subtasks have completed, notify the coordinator", taskInfo.getTaskNameWithSubtasks());
            transition(Stage.NON_SCALE);
            completeFuture.complete(null);
        }
    }

    public void notifyAllRemoteConfirmed(Integer keyGroup, int fromTaskIndex) {
        LOG.info("{} notifyAllRemoteConfirmed for keyGroup: {}", taskInfo.getTaskNameWithSubtasks(), keyGroup);
        if (completedInKeys.contains(keyGroup)) {
            completedInKeys.remove(keyGroup);
            subscaleTracker.notifyMigratedIn(keyGroup, keyToSubscaleID.get(keyGroup), fromTaskIndex);
        } else {
            allConfirmedInKeys.add(keyGroup);
        }

        if (completedInKeys.isEmpty() && completeFuture != null && checkAllSubscaleComplete()) {
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
                    }else if (completedOutKeys.containsKey(keyGroup)) {
                        relatedTasks.add(completedOutKeys.get(keyGroup));
                    }
                }
        );
        return relatedTasks;
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
        boolean completed = pendingOutKeys.isEmpty() && completedOutKeys.isEmpty()
                && pendingInKeys.isEmpty() && completedInKeys.isEmpty();
        if (!completed) {
            LOG.info(
                    "{} checkComplete failed, pendingOutKeys: {}, completedOutKeys: {}, pendingInKeys: {}, completedInKeys: {}",
                    taskInfo.getTaskNameWithSubtasks(),
                    pendingOutKeys,
                    completedOutKeys,
                    pendingInKeys,
                    completedInKeys);
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
