package org.apache.flink.streaming.examples.nexmark;

import org.apache.commons.lang3.RandomUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.streaming.examples.nexmark.NexmarkEvent.*;


public class Query8 {
    public void run(NexmarkCLI cli) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        DataStream<Person> personStream = env.fromSource(
                        KafkaSource
                                .<NexmarkEvent>builder()
                                .setBootstrapServers(cli.kafkaBrokers)
                                .setTopics("nexmark-person")
                                .setGroupId("nexmark")
                                .setDeserializer(new NexmarkEventDeserializationSchema(cli.markerInterval,cli.copyInSource))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "Person Source").setParallelism(3).slotSharingGroup("kafka-source1")
                .shuffle()
                .map(event -> (Person) event).setParallelism(4).slotSharingGroup("kafka-source1").name("PersonMap");

        DataStream<Auction> auctionStream = env.fromSource(
                        KafkaSource
                                .<NexmarkEvent>builder()
                                .setBootstrapServers(cli.kafkaBrokers)
                                .setTopics("nexmark-auction")
                                .setGroupId("nexmark")
                                .setDeserializer(new NexmarkEventDeserializationSchema(cli.markerInterval,cli.copyInSource))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "Auction Source").setParallelism(5).slotSharingGroup("kafka-source2")
                .shuffle()
                .map(event -> (Auction) event).setParallelism(6).slotSharingGroup("kafka-source2").name("AuctionMap");


        DataStream<Person> windowedPersonStream = personStream
                .keyBy(person->person.id)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(cli.windowSizeInSec), Time.milliseconds(cli.slideIntervalInMs)))
                .apply(new WindowFunction<Person, Person, Long, TimeWindow>() {
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<Person> values, Collector<Person> out) throws Exception {
                        for (Person person : values) {
                            // Collect (id, name, window_start)
                            out.collect(person);
                        }
                    }
                }).name("windowedPerson")
                .slotSharingGroup("window-group1")
                .setParallelism(2);

        DataStream<Auction> windowedAuctionStream = auctionStream
                .keyBy(auction->auction.seller)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(cli.windowSizeInSec), Time.milliseconds(cli.slideIntervalInMs)))
                .apply(new WindowFunction<Auction, Auction, Long, TimeWindow>() {
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<Auction> values, Collector<Auction> out) throws Exception {
                        for (Auction auction : values) {
                            // Collect (seller, window_start, window_end)
                            out.collect(auction);
                        }
                    }
                }).name("windowedAuction")
                .slotSharingGroup("window-group2")
                .setParallelism(5);


        DataStream<Tuple2<Long, Long>> joinedStream = windowedPersonStream
                .join(windowedAuctionStream)
                .where(person -> person.id)
                    .equalTo(auction -> auction.seller)
                    .window(SlidingProcessingTimeWindows.of(Time.seconds(cli.windowSizeInSec), Time.milliseconds(cli.slideIntervalInMs)))
                .apply(new JoinFunction<Person, Auction, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> join(Person person, Auction auction) {
                        // Join (person_id, auction_end_time)
                        return new Tuple2<>(person.id, System.currentTimeMillis());
                    }
                });


        FileSink<Tuple2<Long,Long>> localFileSink = FileSink
                .forRowFormat(new Path("file:///opt/output"), new PrintStyleEncoder())
                .build();
        final int perPrint = cli.perPrint;
        joinedStream.keyBy(tuple -> tuple.f0).filter(tuple -> RandomUtils.nextInt(0, perPrint) == 0)
                .sinkTo(localFileSink).name("FileSink").setParallelism(cli.specificParallelism).slotSharingGroup("file-sink");

        env.execute("Nexmark Query8");
    }

    private static class PrintStyleEncoder implements Encoder<Tuple2<Long,Long>> {
        @Override
        public void encode(Tuple2<Long,Long> value, OutputStream stream) throws IOException {
            String formattedString = "creationTime=" + value.f0 +
                    ", outputTime=" + value.f1 + "\n";
            stream.write(formattedString.getBytes(StandardCharsets.UTF_8));
        }
    }
}
