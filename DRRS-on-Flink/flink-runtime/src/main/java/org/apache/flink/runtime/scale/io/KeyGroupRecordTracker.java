package org.apache.flink.runtime.scale.io;

import org.apache.flink.runtime.scale.ScaleConfig;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KeyGroupRecordTracker {

    static final boolean subscale_scheduling_enabled = ScaleConfig.Instance.ENABLE_SUBSCALE_SCHEDULING;

    private long[] keyGroupStatistic;
    private ShortTermRateEstimator shortTermRateEstimator;

    public KeyGroupRecordTracker(int maxKeyGroup, boolean needEstimateRate) {
        if (!subscale_scheduling_enabled) {
            return;
        }

        keyGroupStatistic = new long[maxKeyGroup]; // default value is 0
        if (needEstimateRate) {
            shortTermRateEstimator = new ShortTermRateEstimator(maxKeyGroup);
        } else {
            shortTermRateEstimator = null;
        }
    }

    public void clear() {
        if (!subscale_scheduling_enabled) {
            return;
        }

        // clear counts
        Arrays.fill(keyGroupStatistic, 0);
        if (shortTermRateEstimator != null) {
            shortTermRateEstimator.clear();
        }
    }

    // since the parent class do not have a shutdown method,
    // so this method is not called
    // we just leave the thread here, since the whole JVM will be restarted for each running.
    public void shutdown() {
        if (shortTermRateEstimator != null) {
            shortTermRateEstimator.shutdown();
        }
    }

    public void updateInputRecordMetrics(int keyGroup){
        if (!subscale_scheduling_enabled|| keyGroup < 0){
            return;
        }

        keyGroupStatistic[keyGroup]++;
        if (shortTermRateEstimator != null) {
            shortTermRateEstimator.update(keyGroup);
        }
    }

    // another thread:only read metrics
    public Long[] getTotalCounts(){
        if (!subscale_scheduling_enabled) {
            throw new IllegalStateException("Subscale scheduling is not enabled.");
        }

        // return a deep copy of the metrics
        Long[] copy = new Long[keyGroupStatistic.length];
        for (int i = 0; i < keyGroupStatistic.length; i++) {
            copy[i] = keyGroupStatistic[i];
        }
        return copy;
    }

    public Double[] getRates(){
        if (!subscale_scheduling_enabled) {
            throw new IllegalStateException("Subscale scheduling is not enabled.");
        }

        checkNotNull(shortTermRateEstimator, "Rate estimator is not enabled.");
        return shortTermRateEstimator.getRates();
    }

    // short term rate estimator
    private static class ShortTermRateEstimator {
        private static final int UPDATE_INTERVAL_MS = 100; // Update window every 100ms
        private static final int BUCKET_SIZE_MS = 1_000;  // 1 second buckets
        private static final int WINDOW_SIZE_MS = 5_000;  // 5 seconds window
        private static final int NUM_BUCKETS = WINDOW_SIZE_MS / BUCKET_SIZE_MS;

        private final int maxKeyGroup;
        private final AtomicLong[] currentBucketCounters;

        private final long[][] buckets;
        private volatile int currentBucket;
        private long lastUpdateTime;

        private transient ScheduledExecutorService windowUpdater;

        ShortTermRateEstimator(int maxKeyGroup) {
            this.maxKeyGroup = maxKeyGroup;
            this.currentBucketCounters = new AtomicLong[maxKeyGroup];
            for (int i = 0; i < maxKeyGroup; i++) {
                this.currentBucketCounters[i] = new AtomicLong(0);
            }

            this.currentBucket = 0;
            this.lastUpdateTime = System.currentTimeMillis();
            this.buckets = new long[maxKeyGroup][NUM_BUCKETS];

            this.windowUpdater = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "RateEstimator-WindowUpdater");
                t.setDaemon(true);
                return t;
            });

            this.windowUpdater.scheduleAtFixedRate(
                    this::updateWindowAsync,
                    UPDATE_INTERVAL_MS,
                    UPDATE_INTERVAL_MS,
                    TimeUnit.MILLISECONDS
            );
        }

        private void update(int keyGroup) {
            currentBucketCounters[keyGroup].incrementAndGet();
        }

        public Double[] getRates() {
            long[] totalCounts = new long[maxKeyGroup];

            // Sum up all buckets (buckets array is effectively immutable during read)
            for (int i = 0; i < maxKeyGroup; i++) {
                for (int j = 0; j < NUM_BUCKETS; j++) {
                    totalCounts[i] += buckets[i][j];
                }
            }

            // Add current counter
            for (int i = 0; i < maxKeyGroup; i++) {
                totalCounts[i] += currentBucketCounters[i].get();
            }
            Double[] copy = new Double[totalCounts.length];
            for (int i = 0; i < totalCounts.length; i++) {
                copy[i] = totalCounts[i] / (WINDOW_SIZE_MS / 1000.0);
            }
            return copy;
        }

        // Called by background thread only
        private void updateWindowAsync() {
            try {
                long now = System.currentTimeMillis();
                long timeSinceLastUpdate = now - lastUpdateTime;

                if (timeSinceLastUpdate >= BUCKET_SIZE_MS) {
                    int bucketsToAdvance = (int) (timeSinceLastUpdate / BUCKET_SIZE_MS);
                    int oldBucket = currentBucket;

                    // Move window and reset buckets
                    for (int i = 0; i < Math.min(bucketsToAdvance, NUM_BUCKETS); i++) {
                        int newBucket = (oldBucket + 1) % NUM_BUCKETS;

                        // Update all key groups
                        for (int kg = 0; kg < maxKeyGroup; kg++) {
                            // Transfer current counter to bucket
                            long count = currentBucketCounters[kg].getAndSet(0);
                            buckets[kg][oldBucket] = count;
                            // Clear the new bucket
                            buckets[kg][newBucket] = 0;
                        }

                        oldBucket = newBucket;
                    }

                    // Volatile write ensures visibility of bucket updates to query thread
                    currentBucket = oldBucket;
                    lastUpdateTime = now;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void clear() {
            for (int i = 0; i < maxKeyGroup; i++) {
                for (int j = 0; j < NUM_BUCKETS; j++) {
                    buckets[i][j] = 0;
                }
                currentBucketCounters[i].set(0);
            }
            lastUpdateTime = System.currentTimeMillis();
        }

        public void shutdown() {
            windowUpdater.shutdown();
            try {
                if (!windowUpdater.awaitTermination(1, TimeUnit.SECONDS)) {
                    windowUpdater.shutdownNow();
                }
            } catch (InterruptedException e) {
                windowUpdater.shutdownNow();
            }
        }

    }
}
