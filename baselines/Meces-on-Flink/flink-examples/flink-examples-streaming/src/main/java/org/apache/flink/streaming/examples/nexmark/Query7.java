package org.apache.flink.streaming.examples.nexmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.examples.nexmark.NexmarkEvent.*;

import org.apache.flink.api.common.serialization.Encoder;

import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class Query7 {
    public void run(NexmarkCLI cli) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        WindowStagger windowStagger = WindowStagger.RANDOM;
        if (cli.windowStagger.equals("ALIGNED")) {
            windowStagger = WindowStagger.ALIGNED;
        }else if (cli.windowStagger.equals("NATURAL")) {
            windowStagger = WindowStagger.NATURAL;
        }


        DataStream<Bid> bidStream = env.fromSource(
                        KafkaSource
                                .<NexmarkEvent>builder()
                                .setBootstrapServers(cli.kafkaBrokers)
                                .setTopics("nexmark-bid")
                                .setGroupId("nexmark")
                                .setDeserializer(new NexmarkEventDeserializationSchema(cli.markerInterval,cli.copyInSource))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "Bid Source").slotSharingGroup("kafka-source").setParallelism(cli.specificParallelism)
                .shuffle()
                .map(event -> (Bid) event).name("map").setParallelism(cli.specificParallelism +6).slotSharingGroup("kafka-source").disableChaining();


        DataStream<Tuple2<Long,Long>> result = bidStream
                .keyBy(bid -> bid.bidder)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(cli.windowSizeInSec), Time.milliseconds(cli.slideIntervalInMs)))
                .enableHierarchicalState()
                .apply(
                        new WindowFunction<Bid, Tuple2<Long,Long>, Long, TimeWindow>() {
                            @Override
                            public void apply(Long key, TimeWindow window, Iterable<Bid> values, Collector<Tuple2<Long,Long>> out) {
                                long maxPrice = Long.MIN_VALUE;
                                for (Bid bid : values) {
                                    if (bid.price > maxPrice) {
                                        maxPrice = bid.price;
                                    }
                                }
                                for (Bid bid : values) {
                                    if (bid.price == maxPrice) {
                                        bid.setOutTime();
                                        out.collect(new Tuple2<>(bid.creationTime, bid.outputTime));
                                    }
                                }
                            }
                        }
                ).setMaxParallelism(cli.maxKeyNum).name("maxWindow").slotSharingGroup("max-window");

        FileSink<Tuple2<Long,Long>> localFileSink = FileSink
                .forRowFormat(new Path("file:///opt/output"), new PrintStyleEncoder())
                .build();

        result.sinkTo(localFileSink).name("FileSink").setParallelism(1).slotSharingGroup("file-sink");
        env.execute("Nexmark Query7");
    }

    private static class PrintStyleEncoder implements Encoder<Tuple2<Long,Long>> {
        @Override
        public void encode(Tuple2<Long,Long> value, OutputStream stream) throws IOException {
            String formattedString = "creationTime=" + value.f0 + ", outputTime=" + value.f1 + "\n";
            stream.write(formattedString.getBytes(StandardCharsets.UTF_8));
        }
    }

}
