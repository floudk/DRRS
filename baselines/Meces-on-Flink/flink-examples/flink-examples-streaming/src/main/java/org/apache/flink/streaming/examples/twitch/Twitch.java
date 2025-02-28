package org.apache.flink.streaming.examples.twitch;

import org.apache.commons.lang3.RandomUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;


import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.examples.twitch.TwitchEvent.TwitchEventDeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class Twitch {

    static int maxKeyNum;
    static String kafkaBrokers;
    static int markerInterval;

    static int aggParallelism;
    static int sourceParallelism;
    static boolean enableKeyShuffling;

    static int perPrint;

    public static void main(String[] args) throws Exception {
        CLI.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        KafkaSource<TwitchEvent> kafkaSource = KafkaSource.<TwitchEvent>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics("twitch")
                .setGroupId("flink_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new TwitchEventDeserializationSchema(markerInterval))
                .build();

        env.setMaxParallelism(maxKeyNum);

        // 0. Source from Kafka
        DataStream<TwitchEvent> eventStream =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                        .name("Twitch Source")
                        .setParallelism(sourceParallelism)
                        .slotSharingGroup("basic-ratio");

        // 1. Basic Event Counter
        DataStream<Tuple2<String, Integer>> op1_basicCounts = eventStream
                .map(new BasicEventCounter())
                .name("Basic-Event-Counter")
                .uid("op1")
                .setParallelism(sourceParallelism)
                .slotSharingGroup("basic-ratio");

        // 2. view ratio calculation
        DataStream<Tuple2<String, Double>> op2_viewRatio = op1_basicCounts
                .keyBy(t -> t.f0)
                .process(new ViewerRatioCalculator())
                .name("Viewer-Ratio-Calculator")
                .uid("op2")
                .setParallelism(sourceParallelism)
                .slotSharingGroup("basic-ratio");

        // 3. engagement-scorer
        DataStream<Tuple3<String, Double, Integer>> op3_engagement = op2_viewRatio
                .keyBy(t -> t.f0)
                .process(new EngagementScorer())
                .name("Engagement-Scorer")
                .uid("op3")
                .slotSharingGroup("engagement-retention");

        // 4.  retention-analysis
        DataStream<Tuple3<String, Double, Long>> op4_retention = op3_engagement
                .keyBy(t -> t.f0)
                .process(new RetentionAnalyzer())
                .name("Retention-Analyzer")
                .uid("op4")
                .slotSharingGroup("engagement-retention");

        DataStream<Tuple3<String, Double, Long>> preprocessedStream;
        if (enableKeyShuffling) {
            // Add optional cross-stream analysis and shuffle
            preprocessedStream = op4_retention
                    .rebalance()
                    .process(new CrossStreamAnalyzer())
                    .name("Cross-Stream-Analyzer")
                    .uid("op4_5")
                    .slotSharingGroup("engagement-retention");
        } else {
            // Use original stream directly
            preprocessedStream = op4_retention;
        }

        // 5. viewer-loyalty (Test Operator)
        DataStream<Tuple4<String, Double, Long, Integer>> op5_loyalty = preprocessedStream
                .keyBy(t -> t.f0)
                .process(new ViewerLoyaltyCalculator())
                .name("Viewer-Loyalty-Calculator")
                .uid("op5")
                .slotSharingGroup("loyalty");

        // 6. loyalty-trend-analyzer
        DataStream<Tuple3<String, Double, Double>> op6_trend = op5_loyalty
                .keyBy(t -> t.f0)
                .process(new LoyaltyTrendAnalyzer())
                .name("Loyalty-Trend-Analyzer")
                .uid("op6")
                .slotSharingGroup("trend-metrics");

        // 7. metrics-aggregator
        // this operator has high throughput, so we set a higher parallelism specifically for it
        DataStream<Integer> op7_metrics = op6_trend
                .keyBy(t -> t.f0)
                .process(new MetricsAggregator())
                .name("Metrics-Aggregator")
                .uid("op7")
                .setParallelism(aggParallelism)
                .slotSharingGroup("trend-metrics");


        // 8. Print the result
        FileSink<Integer> fileSink = FileSink
                .forRowFormat(new Path("file:///opt/output"), new FileSinkEncoder())
                .build();
        // remove some output to reduce the output pressure
        op7_metrics.filter(t -> RandomUtils.nextInt(0, perPrint) == 0)
                .sinkTo(fileSink)
                .name("FileSink")
                .uid("op8")
                .setParallelism(aggParallelism)
                .slotSharingGroup("trend-metrics");

        env.execute("Twitch");
    }

    // Operator 1: Basic counter for events
    public static class BasicEventCounter implements MapFunction<TwitchEvent, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(TwitchEvent event) {
            return new Tuple2<>(event.streamerName,
                    event.eventType == TwitchEvent.EventType.ENTER ? 1 : -1);
        }
    }

    // Operator 2: Calculate viewer ratio
    public static class ViewerRatioCalculator
            extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Double>> {
        private ValueState<Integer> viewerCount;
        private ValueState<Integer> totalEvents;

        @Override
        public void open(Configuration parameters) {
            viewerCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("viewers", Integer.class, 0));
            totalEvents = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("events", Integer.class, 0));
        }

        @Override
        public void processElement(Tuple2<String, Integer> event, Context ctx,
                                   Collector<Tuple2<String, Double>> out) throws Exception {
            viewerCount.update(viewerCount.value() + event.f1);
            totalEvents.update(totalEvents.value() + 1);

            double ratio = viewerCount.value() / (double) totalEvents.value();
            out.collect(new Tuple2<>(event.f0, ratio));
        }
    }

    // Operator 3: Score engagement based on viewer ratio
    public static class EngagementScorer
            extends KeyedProcessFunction<String, Tuple2<String, Double>, Tuple3<String, Double, Integer>> {
        private ValueState<Double> avgRatio;
        private ValueState<Integer> updateCount;

        @Override
        public void open(Configuration parameters) {
            avgRatio = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("avg-ratio", Double.class, 0.0));
            updateCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("update-count", Integer.class, 0));
        }

        @Override
        public void processElement(Tuple2<String, Double> event, Context ctx,
                                   Collector<Tuple3<String, Double, Integer>> out) throws Exception {
            double currentAvg = avgRatio.value();
            int count = updateCount.value();

            double newAvg = (currentAvg * count + event.f1) / (count + 1);
            avgRatio.update(newAvg);
            updateCount.update(count + 1);

            out.collect(new Tuple3<>(event.f0, newAvg, count + 1));
        }
    }


    // Operator 4: Analyze retention patterns
    public static class RetentionAnalyzer
            extends KeyedProcessFunction<String, Tuple3<String, Double, Integer>,
            Tuple3<String, Double, Long>> {
        private ValueState<Long> lastEventTime;
        private ValueState<Double> retentionScore;

        @Override
        public void open(Configuration parameters) {
            lastEventTime = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-event", Long.class));
            retentionScore = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("retention", Double.class, 0.0));
        }

        @Override
        public void processElement(Tuple3<String, Double, Integer> event, Context ctx,
                                   Collector<Tuple3<String, Double, Long>> out) throws Exception {
            Long currentTime = ctx.timestamp();
            Long previousTime = lastEventTime.value();

            if (previousTime != null) {
                long timeDiff = currentTime - previousTime;
                double newScore = retentionScore.value() + (event.f1 * timeDiff);
                retentionScore.update(newScore);
                out.collect(new Tuple3<>(event.f0, newScore, timeDiff));
            }

            lastEventTime.update(currentTime);
        }
    }

    // Operator 4.5: (optional) Simulate viewer interactions and shuffle
    public static class CrossStreamAnalyzer
            extends ProcessFunction<Tuple3<String, Double, Long>, Tuple3<String, Double, Long>> {

        private static final long serialVersionUID = 1L;
        private transient LinkedList<Double> recentScores;
        private static final int MAX_RECENT = 10;  // Keep only last 10 scores

        @Override
        public void open(Configuration parameters) {
            recentScores = new LinkedList<>();
        }

        @Override
        public void processElement(Tuple3<String, Double, Long> event, Context ctx,
                                   Collector<Tuple3<String, Double, Long>> out) {
            double retentionScore = event.f1;

            // Add current score and maintain size limit
            recentScores.addLast(retentionScore);
            if (recentScores.size() > MAX_RECENT) {
                recentScores.removeFirst();
            }

            // Simple adjustment based on recent average
            double sum = 0;
            int count = 0;
            for (Double score : recentScores) {
                if (score != retentionScore) {  // Exclude current score
                    sum += score;
                    count++;
                }
            }

            // Adjust score slightly based on recent history
            double adjustedRetention = count > 0
                    ? (retentionScore * 0.8 + (sum / count) * 0.2)
                    : retentionScore;

            out.collect(new Tuple3<>(event.f0, adjustedRetention, event.f2));
        }
    }
    // Operator 5 (Test Operator): Calculate viewer loyalty
    public static class ViewerLoyaltyCalculator
            extends KeyedProcessFunction<String, Tuple3<String, Double, Long>,
            Tuple4<String, Double, Long, Integer>> {
        private MapState<String, ViewerStats> viewerStatsMap;  // Maintains 10-100MB state

        private static class ViewerStats {
            double engagementScore;
            long totalWatchTime;
            int eventCount;
            Map<String, Double> retentionHistory;

            public ViewerStats() {
                retentionHistory = new HashMap<>();
            }
        }

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, ViewerStats> mapStateDescriptor = new MapStateDescriptor<>(
                    "viewer-stats", String.class, ViewerStats.class);
            mapStateDescriptor.setEnableHierarchical();
            viewerStatsMap = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void processElement(Tuple3<String, Double, Long> event, Context ctx,
                                   Collector<Tuple4<String, Double, Long, Integer>> out) throws Exception {
            String channelName = event.f0;
            ViewerStats stats = viewerStatsMap.get(channelName);

            if (stats == null) {
                stats = new ViewerStats();
            }

            stats.engagementScore += event.f1;
            stats.totalWatchTime += event.f2;
            stats.eventCount++;
            stats.retentionHistory.put(
                    String.valueOf(ctx.timestamp()),
                    event.f1
            );

            viewerStatsMap.put(channelName, stats);

            double loyaltyScore = calculateLoyaltyScore(stats);
            out.collect(new Tuple4<>(channelName, loyaltyScore,
                    stats.totalWatchTime, stats.eventCount));
        }

        private double calculateLoyaltyScore(ViewerStats stats) {
            return (stats.engagementScore * 0.4 +
                    stats.totalWatchTime * 0.3 +
                    stats.eventCount * 0.3) / 100.0;
        }
    }

    // Operator 6: Analyze loyalty trends
    public static class LoyaltyTrendAnalyzer
            extends KeyedProcessFunction<String, Tuple4<String, Double, Long, Integer>,
            Tuple3<String, Double, Double>> {
        private ValueState<Double> prevLoyalty;
        private ValueState<Double> loyaltyTrend;

        @Override
        public void open(Configuration parameters) {
            prevLoyalty = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("prev-loyalty", Double.class, 0.0));
            loyaltyTrend = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("loyalty-trend", Double.class, 0.0));
        }

        @Override
        public void processElement(Tuple4<String, Double, Long, Integer> event, Context ctx,
                                   Collector<Tuple3<String, Double, Double>> out) throws Exception {
            double trend = event.f1 - prevLoyalty.value();
            double smoothedTrend = loyaltyTrend.value() * 0.7 + trend * 0.3;

            prevLoyalty.update(event.f1);
            loyaltyTrend.update(smoothedTrend);

            out.collect(new Tuple3<>(event.f0, event.f1, smoothedTrend));
        }
    }

    // Operator 7: Final metrics aggregation
    // For reduce the output pressure, we only output transfer integer, instead of Tuple4
    public static class MetricsAggregator
            extends KeyedProcessFunction<String, Tuple3<String, Double, Double>, Integer> {
        private ValueState<Double> avgLoyalty;
        private ValueState<Double> trendSum;
        private ValueState<Integer> updateCount;

        @Override
        public void open(Configuration parameters) {
            avgLoyalty = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("avg-loyalty", Double.class, 0.0));
            trendSum = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("trend-sum", Double.class, 0.0));
            updateCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("update-count", Integer.class, 0));
        }

        @Override
        public void processElement(Tuple3<String, Double, Double> event, Context ctx,
                                   Collector<Integer> out) throws Exception {
            double currentAvg = avgLoyalty.value();
            double currentTrendSum = trendSum.value();
            int count = updateCount.value();

            double newAvg = (currentAvg * count + event.f1) / (count + 1);
            double newTrendSum = currentTrendSum + event.f2;

            avgLoyalty.update(newAvg);
            trendSum.update(newTrendSum);
            updateCount.update(count + 1);

//            out.collect(new Tuple4<>(event.f0, newAvg, newTrendSum / (count + 1), count + 1));
            out.collect(count + 1);
        }
    }

    // operator 8: File Sink
    public static class FileSinkEncoder implements Encoder<Integer> {
        @Override
        public void encode(Integer value, OutputStream stream) throws IOException {
            // f0: channel name, f1: avg loyalty, f2: avg trend, f3: update count
            // in evaluation, we just print necessary information to calculate throughput
            String record = "outputTime=" + System.currentTimeMillis() + '\n';
            stream.write(record.getBytes());
        }
    }


    static class CLI{
        static final String MAX_KEY_NUM = "maxKeyNum";
        static final String KAFKA_BROKERS = "kafkaBrokers";
        static final String MARKER_INTERVAL = "markerInterval";
        static final String SOURCE_PARALLELISM = "sourceParallelism";
        static final String AGG_PARALLELISM = "aggParallelism";

        static void fromArgs(String[] args){
            MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
            maxKeyNum = params.getInt(MAX_KEY_NUM, 128);
            kafkaBrokers = params.get(KAFKA_BROKERS, "broker:9092");
            markerInterval = params.getInt(MARKER_INTERVAL, 100);
            aggParallelism = params.getInt(AGG_PARALLELISM, 4);
            sourceParallelism = params.getInt(SOURCE_PARALLELISM, 4);
            enableKeyShuffling = params.getBoolean("enableKeyShuffling", false);
            perPrint = params.getInt("perPrint", 800);
        }
    }
}
