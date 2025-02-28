package org.apache.flink.streaming.runtime.scale.scheduling;


import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.streaming.runtime.scale.migrate.MigrationBuffer;
import org.apache.flink.runtime.scale.SubscaleTracker.TrackerID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class IntraSubscaleKeyOrderSelector {
    static final Logger LOG = LoggerFactory.getLogger(IntraSubscaleKeyOrderSelector.class);

    Map<TrackerID, InternalSelector> selectors = new HashMap<>();
    private final ScalingContext scalingContext;
    private final MigrationBuffer migrationBuffer;
    private final Map<Integer, Long> stateSizeMap = new HashMap<>();


    public IntraSubscaleKeyOrderSelector(
            ScalingContext scalingContext,
            MigrationBuffer migrationBuffer){
        this.scalingContext = scalingContext;
        this.migrationBuffer = migrationBuffer;
    }

    // main-thread
    public void registerSelectorForSubscale(
            int subscaleID,
            Map<Integer, List<Integer>> requiredKeys,
            Map<Integer,Long> stateSizes) {
        stateSizeMap.putAll(stateSizes);
        requiredKeys.forEach(
                (sourceTaskIndex, keys) ->{
                    TrackerID trackerID = new TrackerID(subscaleID, sourceTaskIndex);
                    selectors.put(trackerID, createSelector(trackerID,keys));
                });
    }

    // async-thread
    public int next(int subscaleID, int sourceTaskIndex){
        return selectors.get(new TrackerID(subscaleID, sourceTaskIndex)).getNext();
    }


    InternalSelector createSelector(TrackerID trackerID ,List<Integer> keys){
        switch (ScaleConfig.Instance.SUBSCALE_INTERNAL_KEY_SCHEDULER){
            case Lexicographic:
                return new LexicographicSelector(trackerID,keys);
            case AdaptiveHeuristic:
                return new AdaptiveSelector(trackerID,keys);
            default:
                throw new RuntimeException("Unknown scale config subscale key selector: " + ScaleConfig.Instance.SUBSCALE_INTERNAL_KEY_SCHEDULER);
        }
    }

    public void close(){
        selectors.clear();
    }


    static abstract class InternalSelector{
        protected Set<Integer> keys;
        protected TrackerID trackerID;

        InternalSelector(TrackerID id, Set<Integer> keys){
            this.trackerID = id;
            this.keys = keys;
        }
        boolean hasNext(){
            return !keys.isEmpty();
        }
        int getNext(){
            if (!hasNext()) {
                return -1;
            }else{
                return next();
            }
        }
        protected abstract int next();
    }
    /**
     * Just use the lexicographic order of the keys
     */
    static class LexicographicSelector extends InternalSelector{
        LexicographicSelector(TrackerID id, List<Integer> keys){
            super(id, new TreeSet<>(keys));
        }
        @Override
        protected int next() {
            return ((TreeSet<Integer>) keys).pollFirst();
        }
    }


    static abstract class HeuristicSelector extends InternalSelector{
        HeuristicSelector(TrackerID id, Set<Integer> keys){
            super(id,keys);
        }
    }
    /**
     * 1. when low suspend probability in current Subscale,
     *    use heuristic considering fairness and efficiency
     * 2. when high suspend probability in current Subscale,
     *    use heuristic considering resuming speed
     */
     class AdaptiveSelector extends HeuristicSelector{
         final double fairnessWeight = ScaleConfig.Instance.FAIRNESS_WEIGHT;
         int lastKeyGroup = -1;
         private static final double EPSILON = 1e-10; // Avoid division by zero

         AdaptiveSelector(TrackerID id, List<Integer> keys){
            super(id, new HashSet<>(keys));
        }
        @Override
        protected int next() {

            Map<Integer,Integer> keyCounts = migrationBuffer.getKeyCounts(keys);

            if (keyCounts.values().stream().allMatch(count -> count == 0)){
                // maybe all keys are not hot
                // return minimal cost key
                int minCostKey = keys.iterator().next();
                long minCost = stateSizeMap.get(minCostKey);
                for (Integer key : keys){
                    long cost = stateSizeMap.get(key);
                    if (cost < minCost){
                        minCost = cost;
                        minCostKey = key;
                    }
                }
                keys.remove(minCostKey);
                lastKeyGroup = minCostKey;
            }else if (checkEmergency()){
                lastKeyGroup = findMinimalCostForResume(keyCounts);
            }else{
                lastKeyGroup = selectFairEfficient(keyCounts);
            }
            return lastKeyGroup;
        }

        boolean checkEmergency(){
             int ongoingMigratingProcess = scalingContext.getOngoingMigratingProcess();
             LOG.info("ongoingMigratingProcess: {}", ongoingMigratingProcess);
             return migrationBuffer.checkEmergency(lastKeyGroup) && scalingContext.getOngoingMigratingProcess() > 1;
        }

        int findMinimalCostForResume(Map<Integer,Integer> keyCounts){
            int minCostKey = keyCounts.keySet().iterator().next();
            long minCost = stateSizeMap.get(minCostKey);
            for (Integer key : keyCounts.keySet()){
                long cost = stateSizeMap.get(key);
                if (cost < minCost){
                    minCost = cost;
                    minCostKey = key;
                }
            }
            LOG.info("findMinimalCostForResume: key: {}, cost: {}", minCostKey, minCost);
            keys.remove(minCostKey);
            return minCostKey;
        }
        int selectFairEfficient(Map<Integer,Integer> keyCounts){
             // fairness: longer waiting time, higher priority
            Map<Integer, Double> waitingTimes  = migrationBuffer.getAverageWaitingTime(keys);
            // efficiency : keyCounts / stateSize, higher efficiency, higher priority
            Map<Integer, Double> efficiencies = new HashMap<>();
            for (Integer key : keys){
                efficiencies.put(key, (double) keyCounts.get(key) / stateSizeMap.get(key));
            }

            // Normalize values to [0,1] range
            Map<Integer, Double> normalizedWaiting = normalizeValues(waitingTimes);
            Map<Integer, Double> normalizedEfficiency = normalizeValues(efficiencies);

            // Find key with maximum weighted priority
            double maxPriority = Double.MIN_VALUE;
            int maxPriorityKey = -1;

            for (Integer key : keys) {
                double priority = fairnessWeight * normalizedWaiting.get(key) +
                        (1 - fairnessWeight) * normalizedEfficiency.get(key);
                if (priority > maxPriority) {
                    maxPriority = priority;
                    maxPriorityKey = key;
                }
            }
            keys.remove(maxPriorityKey);
            LOG.info("Select key: {}, waitingTime: {}, efficiency: {}", maxPriorityKey, waitingTimes.get(maxPriorityKey), efficiencies.get(maxPriorityKey));
            return maxPriorityKey;
        }
        private Map<Integer, Double> normalizeValues(Map<Integer, Double> values) {
            if (values.isEmpty()) {
                return new HashMap<>();
            }

            double minValue = values.values().stream().min(Double::compare).get();
            double maxValue = values.values().stream().max(Double::compare).get();
            double range = maxValue - minValue;

            Map<Integer, Double> normalized = new HashMap<>();

            if (range < EPSILON) {
                // If all values are the same, normalize to 1.0
                for (Map.Entry<Integer, Double> entry : values.entrySet()) {
                    normalized.put(entry.getKey(), 1.0);
                }
            } else {
                // Normalize to [0,1] range
                for (Map.Entry<Integer, Double> entry : values.entrySet()) {
                    double normalizedValue = (entry.getValue() - minValue) / range;
                    normalized.put(entry.getKey(), normalizedValue);
                }
            }

            return normalized;
        }
    }
}
