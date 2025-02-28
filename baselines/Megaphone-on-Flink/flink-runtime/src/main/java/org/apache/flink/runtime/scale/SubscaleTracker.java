package org.apache.flink.runtime.scale;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Only for information purpose.
 */
public class SubscaleTracker {
    static final Logger LOG = LoggerFactory.getLogger(SubscaleTracker.class);

    List<Set<Integer>> cachedUnmodifiedSubscales = new ArrayList<>();
    Map<Integer, Set<Integer>> subscales = new HashMap<>();

    final Consumer<Set<Integer>> subscaleTrackerNotifier;

    public SubscaleTracker(Consumer<Set<Integer>> subscaleTrackerNotifier) {
        this.subscaleTrackerNotifier = subscaleTrackerNotifier;
    }

    public void addSubscale(Map<Integer, List<Integer>> sourceTaskWithInKeys) {
        for ( List<Integer> keyGroups : sourceTaskWithInKeys.values() ) {
            Set<Integer> subscale = new HashSet<>(keyGroups);
            keyGroups.forEach( keyGroup -> subscales.put(keyGroup, subscale));
            // add deep copy of subscale
            cachedUnmodifiedSubscales.add(new HashSet<>(subscale));
        }
    }

    public void notifyMigratedIn(int keyGroup) {
        Set<Integer> subscale = subscales.get(keyGroup);
        checkNotNull(subscale);
        subscale.remove(keyGroup);
        subscales.remove(keyGroup);

        if (subscale.isEmpty()) {
            // one subscale is completed
            int index = 0;
            for (Set<Integer> cachedSubscale : cachedUnmodifiedSubscales) {
                if (cachedSubscale.contains(keyGroup)) {
                    break;
                }
                index++;
            }
            Set<Integer> completedSubscale = cachedUnmodifiedSubscales.remove(index);
            LOG.info("Subscale completed: {}", completedSubscale);
            CompletableFuture.runAsync(() -> subscaleTrackerNotifier.accept(completedSubscale));
        }
    }
}
