package org.apache.flink.runtime.scale;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.scale.state.FlexibleKeyGroupRange;

import org.apache.flink.runtime.scale.state.HierarchicalStateID;

import org.apache.flink.runtime.scale.state.KeyOrStateID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class ScalingContext {
    private static final Logger LOG = LoggerFactory.getLogger(ScalingContext.class);

    private final TaskInfo taskInfo;

    private final int[] parallelismPair; // [oldParallelism, newParallelism]
    // its real holder: keyContext
    private FlexibleKeyGroupRange localKeyGroups;
    // BitSet real holder: HierarchicalStateMap
    private Map<Integer, BitSet> localBins = new HashMap<>();

    // cache only put and never remove
    // only for read task info
    private final Map<Integer,Integer> pendingInKeysCache = new HashMap<>();
    private final Map<Integer, Integer> pendingOutKeysCache = new HashMap<>();
    public final Map<Integer, Integer> earlyMigrateKeys = new HashMap<>();

    // key groups from other tasks that are sent to this task
    public final Map<Integer,Integer> pendingInKeys = new HashMap<>(); // updated by main thread immediately after states merged
    // key groups from this task that are sent to other tasks
    public final Map<Integer, Integer> pendingOutKeys = new HashMap<>(); // keys ready to be sent out


    public final CompletableFuture<Void> initFuture = new CompletableFuture<>();

    private CompletableFuture<Void> completeFuture;

    private BiConsumer<Integer, KeyOrStateID> leftBinHandler;
    private int sourceAligning = 0;

    public AtomicInteger sendingAckCount = new AtomicInteger(0);

    // ------------------------------------------------------------------------------
    public ScalingContext(TaskInfo taskInfo) {
        this.taskInfo = taskInfo;
        // initialize parallelismPair to [-1, -1]
        this.parallelismPair = new int[]{-1, -1};
    }
    public void updateTaskInfo(int newParallelism) {
        parallelismPair[0] = taskInfo.getNumberOfParallelSubtasks();
        parallelismPair[1] = newParallelism;
        LOG.info("{} update task info with parallelisms: {} -> {}", getTaskName(), parallelismPair[0],  parallelismPair[1]);
        taskInfo.scale(newParallelism);
    }
    public void triggered(ContextInitInfo info) {
        transition(Stage.TRIGGERED);
        this.localKeyGroups = info.currentKeyGroups;
        this.localBins = info.currentBins;
        if (isNewlyCreatedTask()) {
            // reset for newly created task: clear local key groups and bins
            info.clearStateRunnable.run();
            this.localBins.clear();
        }
    }

    public void initScale(
            Map<Integer, List<Integer>> sourceTaskWithInKeys,
            Map<Integer, List<Integer>> targetTaskWithOutKeys) {

        LOG.info("{} set subscale context with sourceTaskWithInKeys: {} and targetTaskWithOutKeys: {}, "
                        + "current local key groups: {}",
                getTaskName(),
                sourceTaskWithInKeys,
                targetTaskWithOutKeys,
                localKeyGroups);
        sourceAligning = sourceTaskWithInKeys.size();

        for (Map.Entry<Integer, List<Integer>> entry : sourceTaskWithInKeys.entrySet()) {
            for (int keyGroup : entry.getValue()) {
                pendingInKeys.put(keyGroup, entry.getKey());
                pendingInKeysCache.put(keyGroup, entry.getKey());
            }
        }

        for (Map.Entry<Integer, List<Integer>> entry : targetTaskWithOutKeys.entrySet()) {
            for (int keyGroup : entry.getValue()) {
                pendingOutKeys.put(keyGroup, entry.getKey());
                pendingOutKeysCache.put(keyGroup, entry.getKey());
            }
        }

        initFuture.complete(null);
    }

    public int getNewParallelism() {
        return parallelismPair[1];
    }
    public boolean isNewlyCreatedTask() {
        checkArgument(parallelismPair[0] != -1 && parallelismPair[1] != -1,
                "parallelismPair is not initialized");
        // only newly created task has parallelismPair[0] == parallelismPair[1]
        // since when the task is created, the parallelism has already been set to the new value
        return parallelismPair[0] == parallelismPair[1];
    }

    public int getRelatedTask(int keyGroupIndex){
        if (pendingInKeysCache.containsKey(keyGroupIndex)) {
            return pendingInKeysCache.get(keyGroupIndex);
        }
        if (pendingOutKeysCache.containsKey(keyGroupIndex)) {
            return pendingOutKeysCache.get(keyGroupIndex);
        }
        if (earlyMigrateKeys.containsKey(keyGroupIndex)){
            LOG.info("process early migrate key group {}", keyGroupIndex);
            return earlyMigrateKeys.get(keyGroupIndex);
        }
        LOG.error("key group {} is neither in pendingOutKeysCache {} nor pendingInKeysCache {}",
                keyGroupIndex, pendingOutKeysCache, pendingInKeysCache);
        throw new IllegalArgumentException("key group " + keyGroupIndex + " is not in pendingOutKeysCache");
    }


    // ------------------------------------------------------------------------------
    private AvailabilityProvider.AvailabilityHelper suspendingChecker;
    public void setSuspendingCheckerAndBinHandler(
            AvailabilityProvider.AvailabilityHelper availabilityHelper,
            BiConsumer<Integer, KeyOrStateID> leftBinHandler) {
        this.suspendingChecker = availabilityHelper;
        this.leftBinHandler = leftBinHandler;
    }

    public AtomicInteger transferringCount = new AtomicInteger(0); // to avoid close channel while still transferring

    private boolean aligned = false;

    public void markAligned(){
        checkState(!aligned, "already aligned");
        aligned = true;
        tryCompleteScale();
    }
    public boolean isAligned(){
        return aligned;
    }

    public void onMerge(KeyOrStateID keyOrStateID, BitSet bins) {
        int keyGroupIndex = keyOrStateID.getKey();
        localBins.put(keyGroupIndex, bins); // update or add local bins

        boolean checkComplete = (bins.cardinality() == ScaleConfig.Instance.HIERARCHICAL_BIN_NUM);

        if (keyOrStateID.isKey()){
            checkNotNull(pendingInKeys.remove(keyGroupIndex), "key group %s is not in pendingInKeys", keyGroupIndex);
            checkComplete &= pendingInKeys.isEmpty();
        }

        if (checkComplete && suspendingChecker.isAvailable()){
            tryCompleteScale();
        }
    }

    public void startMigrate(KeyOrStateID keyOrStateID, boolean clearBins, int targetTask) {
        if (clearBins){
            localBins.remove(keyOrStateID.getKey());
            if (keyOrStateID.isKey()){
                checkNotNull(pendingOutKeys.remove(keyOrStateID.getKey()),"  key group %s is not in pendingOutKeys", keyOrStateID.getKey());
            }
        }else{
            BitSet bins = localBins.get(keyOrStateID.getKey());
            if(bins.get(keyOrStateID.getStateID().binIndex)){
                LOG.error("bin {} is still local: {}({})", keyOrStateID.getStateID(), bins, System.identityHashCode(bins));
                throw new IllegalArgumentException("bin " + keyOrStateID.getStateID() + " is still local");
            }
        }
        transferringCount.incrementAndGet();
        // may receive fetch before subscale triggered
        if (!pendingOutKeysCache.containsKey(keyOrStateID.getKey()) && !pendingInKeysCache.containsKey(keyOrStateID.getKey())){
            LOG.info("receive fetch before subscale triggered, key group: {}", keyOrStateID.getKey());
            earlyMigrateKeys.put(keyOrStateID.getKey(), targetTask);
        }
    }


    private boolean checkAllLocalBinFull(){
        for (Map.Entry<Integer, BitSet> entry : localBins.entrySet()) {
            if (entry.getValue().cardinality() != ScaleConfig.Instance.HIERARCHICAL_BIN_NUM){
                LOG.info("key group {} is not fully migrated out, bins: {}", entry.getKey(), entry.getValue());
                return false;
            }
        }
        return true;
    }

    // which will never need, since the key group is already migrated out and aligned
    private void tryHandleLeftOutBinsAfterAligned(){

        checkNotNull(leftBinHandler, "leftBinHandler is not set");
        HierarchicalStateID leftOutBin = null;
        for (Map.Entry<Integer, BitSet> entry : localBins.entrySet()) {
            int keyGroupIndex = entry.getKey();
            if (pendingOutKeysCache.containsKey(keyGroupIndex)){
                BitSet bins = entry.getValue();
                if (bins.nextSetBit(0) != -1){
                    leftOutBin = new HierarchicalStateID(keyGroupIndex, bins.nextSetBit(0));
                    break;
                }
            }
        }
        if (leftOutBin == null){
            return;
        }
        LOG.info("try handle left out bin {}", leftOutBin);
        leftBinHandler.accept(
                getRelatedTask(leftOutBin.keyGroupIndex), new KeyOrStateID(leftOutBin));
    }


    public void tryCompleteScale(){
        if(status ==Stage.NON_SCALE){
            return;
        }
        boolean completed = aligned && status == Stage.COMPLETING && transferringCount.get() == 0 && sendingAckCount.get() == 0
                && pendingOutKeys.isEmpty() && pendingInKeys.isEmpty() && suspendingChecker.isAvailable()
                && sourceAligning == 0;
        completed &= checkAllLocalBinFull();
        if (completed) {
            LOG.info("{} all subtasks have completed, notify the coordinator", getTaskName());
            transition(Stage.NON_SCALE);
            completeFuture.complete(null);
        }else{
            LOG.info("try complete scale failed, aligned: {}, transferringCount: {}, pendingOutKeys: {}, pendingInKeys: {}, no-suspending: {}, sourceAligning: {}, sendingAckCount: {}",
                    aligned, transferringCount.get(), pendingOutKeys, pendingInKeys, suspendingChecker.isAvailable(), sourceAligning, sendingAckCount.get());
            if (pendingOutKeys.isEmpty() && aligned && !pendingOutKeysCache.isEmpty()){
                tryHandleLeftOutBinsAfterAligned();
            }
        }
    }

    public void logInfoDuringError() {
        LOG.error("{} pendingInKeys: {}, pendingOutKeys: {}",
                getTaskName(), pendingInKeys, pendingOutKeys);
        LOG.error("{} localKeyGroups: {}", getTaskName(), localKeyGroups);
        List<Integer> fullyLocalKeyGroups = new ArrayList<>();
        for (Map.Entry<Integer, BitSet> entry : localBins.entrySet()) {
            int keyGroup = entry.getKey();
            BitSet bins = entry.getValue();
            if (bins.cardinality() == ScaleConfig.Instance.HIERARCHICAL_BIN_NUM){
                fullyLocalKeyGroups.add(keyGroup);
            }else{
                LOG.error("{} key group {} is not fully local, bins: {}", getTaskName(), keyGroup, bins);
            }
        }
        LOG.error("{} fully local key groups: {}", getTaskName(), fullyLocalKeyGroups);
    }




    // ---------------------------------------------------------------------------
    public boolean isLocalBin(HierarchicalStateID stateID) {
        if (localKeyGroups.contains(stateID.keyGroupIndex) ^ localBins.containsKey(stateID.keyGroupIndex)){
            LOG.warn("{} key group {} not match local key groups {} and local bins {}",
                    getTaskName(),
                    stateID,
                    localKeyGroups,
                    localBins.keySet());
        }

        return localKeyGroups.contains(stateID.keyGroupIndex)
                && localBins.containsKey(stateID.keyGroupIndex)
                    && localBins.get(stateID.keyGroupIndex).get(stateID.binIndex);
    }



    public void registerCompleteNotifier(Runnable completeNotifier) {
        transition(Stage.COMPLETING);
        // wait for the completion of all subtasks
        completeFuture = new CompletableFuture<>();
        completeFuture.thenRun(completeNotifier);
    }


    public boolean isTimeServiceClearNeeded(KeyOrStateID keyOrStateID){
        return keyOrStateID.isKey() || isLastBin(keyOrStateID.getStateID());
    }

    public boolean isLastBin(HierarchicalStateID stateID){
        return localBins.get(stateID.keyGroupIndex).cardinality() == 1;
    }

    public void markSourceAligned(int eventSenderIndex) {
        LOG.info("source task {} aligned", eventSenderIndex);
        sourceAligning--;
        if (sourceAligning == 0){
            tryCompleteScale();
        }
    }

    public Set<Integer> getAllTargetTasks() {
        return new HashSet<>(pendingOutKeysCache.values());
    }

    public boolean isMigratedOutKey(int keyGroupIndex){
        return pendingOutKeysCache.containsKey(keyGroupIndex) && !pendingOutKeys.containsKey(keyGroupIndex);
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
        LOG.info("{} status transition from {} to {}", getTaskName(), oldStatus, status);
    }

    @Override
    public String toString() {
        // Stage
        // pendingInKeys
        // pendingOutKeys
        return "Stage: " + status + "--"
                + "pendingInKeys: " + pendingInKeys + "--"
                + "pendingOutKeys: " + pendingOutKeys;

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

    static public class ContextInitInfo{
        public final FlexibleKeyGroupRange currentKeyGroups;
        public final Map<Integer, BitSet> currentBins;
        public final Runnable clearStateRunnable;
        public ContextInitInfo(
                FlexibleKeyGroupRange currentKeyGroups,
                Map<Integer, BitSet> currentBins,
                Runnable clearStateRunnable) {
            this.currentKeyGroups = currentKeyGroups;
            this.currentBins = currentBins;
            this.clearStateRunnable = clearStateRunnable;
        }
    }
}
