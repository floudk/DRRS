package org.apache.flink.runtime.scale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Only for information purpose.
 */
public class SubscaleTracker {
    static final Logger LOG = LoggerFactory.getLogger(SubscaleTracker.class);

    Map<TrackerID, Set<Integer>> cachedUnmodifiedSubscales = new HashMap<>();

    Map<TrackerID, Set<Integer>> transferInTracker = new HashMap<>();
    Map<TrackerID, Set<Integer>> migratedInTracker = new HashMap<>();

    Map<Integer, Set<TrackerID>> subscaleToTrackers = new HashMap<>();

    final Consumer<Set<Integer>> inGroupTrackerNotifier;
    final Consumer<Integer> channelCloseNotifier;

    public SubscaleTracker(
            Consumer<Set<Integer>> inGroupTrackerNotifier,
            Consumer<Integer> channelCloseNotifier) {
        this.inGroupTrackerNotifier = inGroupTrackerNotifier;
        this.channelCloseNotifier = channelCloseNotifier;
    }

    public void addSubscale(Map<Integer, List<Integer>> sourceTaskWithInKeys, int subscaleID) {
        for ( Map.Entry<Integer, List<Integer>> entry : sourceTaskWithInKeys.entrySet() ) {
            int sourceTaskIndex = entry.getKey();
            List<Integer> keyGroups = entry.getValue();
            transferInTracker.put(new TrackerID(subscaleID, sourceTaskIndex), new HashSet<>(keyGroups));
            migratedInTracker.put(new TrackerID(subscaleID, sourceTaskIndex), new HashSet<>(keyGroups));
            cachedUnmodifiedSubscales.put(new TrackerID(subscaleID, sourceTaskIndex), new HashSet<>(keyGroups));
            subscaleToTrackers.computeIfAbsent(subscaleID, k -> new HashSet<>()).add(new TrackerID(subscaleID, sourceTaskIndex));
        }
    }

    public void notifyTransferredIn(int keyGroup, int subscaleID, int sourceTaskIndex) {
        // LOG.info("Notify state {} transferred in with subscaleID {}", keyGroup, subscaleID);
        TrackerID trackerID = new TrackerID(subscaleID, sourceTaskIndex);
        Set<Integer> inGroup = transferInTracker.get(trackerID);
        checkNotNull(inGroup, "TrackerID %s-%s not found", subscaleID, sourceTaskIndex);
        inGroup.remove(keyGroup);
        if (inGroup.isEmpty()){
            transferInTracker.remove(trackerID);
            //LOG.info("InGroup completed: {}", subscaleID);
            Set<Integer> cachedInGroup = cachedUnmodifiedSubscales.remove(trackerID);
            CompletableFuture.runAsync(() -> inGroupTrackerNotifier.accept(cachedInGroup));
        }
    }

    public void notifyMigratedIn(int keyGroup, int subscaleID, int sourceTaskIndex) {
        LOG.info("Notify state {} migrated in with subscaleID {}", keyGroup, subscaleID);
        TrackerID trackerID = new TrackerID(subscaleID, sourceTaskIndex);
        Set<Integer> inGroup = migratedInTracker.get(trackerID);
        checkNotNull(inGroup, "TrackerID %s-%s not found", subscaleID, sourceTaskIndex);
        inGroup.remove(keyGroup);
        if (inGroup.isEmpty()){
            migratedInTracker.remove(trackerID);
            Set<TrackerID> trackerIDs = subscaleToTrackers.get(subscaleID);
            trackerIDs.remove(trackerID);
            if (trackerIDs.isEmpty()){
                LOG.info("All in keys completed in subscale {}", subscaleID);
                subscaleToTrackers.remove(subscaleID);
                if (ScaleConfig.Instance.ENABLE_SUBSCALE){
                    CompletableFuture.runAsync(() -> channelCloseNotifier.accept(subscaleID));
                }
            }
        }
    }






    static class TrackerID{
        public int subscaleID;
        public int sourceTaskIndex;
        public TrackerID(int subscaleID, int sourceTaskIndex) {
            this.subscaleID = subscaleID;
            this.sourceTaskIndex = sourceTaskIndex;
        }
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TrackerID){
                TrackerID other = (TrackerID) obj;
                return this.subscaleID == other.subscaleID && this.sourceTaskIndex == other.sourceTaskIndex;
            }
            return false;
        }
        @Override
        public int hashCode() {
            return subscaleID * 31 + sourceTaskIndex;
        }
    }
}
