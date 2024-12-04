package org.apache.flink.streaming.examples.nexmark;

import org.apache.flink.api.java.utils.MultipleParameterTool;

public class NexmarkCLI {
    static final String QUERY = "query";
    static final String KAFKA_BROKERS = "kafkaBrokers";
    static final String WINDOW_SIZE = "windowSize";
    static final String MAX_KEY_NUM = "maxKeyNum";
    static final String MARKER_INTERVAL = "markerInterval";
    static final String COPY_IN_SOURCE = "copyInSource";
    static final String SPECIFIC_PARALLELISM = "specificParallelism";
    static final String WINDOW_STAGGER = "windowStagger";
    static final String SLIDE_INTERVAL = "slideIntervalInMs";


    String query;
    String kafkaBrokers;
    int windowSizeInSec;
    int maxKeyNum;
    int markerInterval;
    int copyInSource;
    public int specificParallelism;
    String windowStagger;
    public long slideIntervalInMs;


    private NexmarkCLI(
            String kafkaBrokers,
            String query,
            int maxKeyNum,
            int windowSizeInSec,
            int markerInterval,
            int copyInSource,
            int specificParallelism,
            String windowStagger,
            long slideIntervalInMs) {
        this.kafkaBrokers = kafkaBrokers;
        this.query = query;
        this.windowSizeInSec = windowSizeInSec;
        this.maxKeyNum = maxKeyNum;
        this.markerInterval = markerInterval;
        this.copyInSource = copyInSource;
        this.specificParallelism = specificParallelism;
        this.windowStagger = windowStagger;
        this.slideIntervalInMs = slideIntervalInMs;
    }
    public static NexmarkCLI fromArgs(String[] args) {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        String query = null;
        String kafkaBrokers = null;
        int windowSizeInSec = -1;
        int maxKeyNum = -1;
        int markerInterval = -1;
        int copyInSource = -1;

        if (params.has(QUERY)){
            query = params.get(QUERY);
        }

        if (params.has(KAFKA_BROKERS)) {
            kafkaBrokers = params.get(KAFKA_BROKERS);
        }
        if (params.has(WINDOW_SIZE)) {
            windowSizeInSec = params.getInt(WINDOW_SIZE);
        }
        if (params.has(MAX_KEY_NUM)) {
            maxKeyNum = params.getInt(MAX_KEY_NUM);
        }
        if (params.has(MARKER_INTERVAL)) {
            markerInterval = params.getInt(MARKER_INTERVAL);
        }

        if (params.has(COPY_IN_SOURCE)) {
            copyInSource = params.getInt(COPY_IN_SOURCE);
        }

        int parallelism = params.getInt(SPECIFIC_PARALLELISM, 4);
        String windowStagger = params.get(WINDOW_STAGGER, "RANDOM");
        long slideIntervalInMs = params.getLong(SLIDE_INTERVAL, 1000);

        return new NexmarkCLI(kafkaBrokers, query, maxKeyNum, windowSizeInSec, markerInterval, copyInSource, parallelism, windowStagger, slideIntervalInMs);
    }
}
