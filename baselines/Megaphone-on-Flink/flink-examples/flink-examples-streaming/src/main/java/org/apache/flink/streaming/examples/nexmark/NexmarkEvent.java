package org.apache.flink.streaming.examples.nexmark;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.runtime.scale.ScaleEvaEvent;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Random;

public abstract class NexmarkEvent extends ScaleEvaEvent {
    public abstract NexmarkEvent perturbClone();

    static public class Marker extends NexmarkEvent {
        Marker(long creationTime) {
            isMarker = true;
            this.creationTime = creationTime;
        }
        @Override
        public NexmarkEvent perturbClone() {
            return new Marker(creationTime);
        }
    }

    @Override
    public String toString() {
        return "NexmarkEvent{" +
                "creationTime=" + creationTime +
                ", outputTime=" + outputTime +
                '}';
    }

    static public class Person extends NexmarkEvent{
        public final static int EVENT_ID = 0;

        public long id;
        public String name;
        public String emailAddress;
        public String creditCard;
        public String city;
        public String state;
        public String extra;

        @JsonCreator
        public Person(
                @JsonProperty("id") long id,
                @JsonProperty("name") String name,
                @JsonProperty("email") String emailAddress,
                @JsonProperty("creditCard") String creditCard,
                @JsonProperty("city") String city,
                @JsonProperty("state") String state,
                @JsonProperty("extra") String extra) {
            this.id = id;
            this.name = name;
            this.emailAddress = emailAddress;
            this.creditCard = creditCard;
            this.city = city;
            this.state = state;
            this.extra = extra;
        }
        @Override
        public Person perturbClone() {
            return new Person(id, name, emailAddress, creditCard, city, state, extra);
        }
    }

    static public class Auction extends NexmarkEvent{
        static final int EVENT_ID = 1;

        public long id;
        public long seller;
        public long category;

        public long initialBid;
        public long expires;

        public String name;
        public String description;

        public long reserve;
        public String extra;


        @JsonCreator
        public Auction(
                @JsonProperty("id") long id,
                @JsonProperty("seller") long seller,
                @JsonProperty("category") long category,
                @JsonProperty("initialBid") long initialBid,
                @JsonProperty("expires") long expires,
                @JsonProperty("name") String name,
                @JsonProperty("description") String description,
                @JsonProperty("reserve") long reserve,
                @JsonProperty("extra") String extra
        ) {
            this.id = id;
            this.seller = seller;
            this.category = category;
            this.initialBid = initialBid;
            this.expires = expires;
            this.name = name;
            this.description = description;
            this.reserve = reserve;
            this.extra = extra;
        }

        @Override
        public Auction perturbClone() {
            return new Auction(id, seller, category, initialBid, expires, name, description, reserve, extra);
        }

    }

    static public class Bid extends NexmarkEvent{
        static final int EVENT_ID = 2;

        public long auction;
        public long bidder;
        public long price;
        public String channel;
        public String url;
        public String extra;

        @JsonCreator
        public Bid(
                @JsonProperty("auction") long auction,
                @JsonProperty("bidder") long bidder,
                @JsonProperty("price") long price,
                @JsonProperty("channel") String channel,
                @JsonProperty("url") String url,
                @JsonProperty("extra") String extra) {
            this.auction = auction;
            this.bidder = bidder;
            this.price = price;
            this.channel = channel;
            this.url = url;
            this.extra = extra;
        }

        @Override
        public String toString() {
            return "Bid[" + auction + ", " + bidder + ", " + price + ", " + channel + ", " + url
                    + "]";
        }
        @Override
        public Bid perturbClone() {
            long newBidder = (bidder - NexmarkEventDeserializationSchema.random.nextInt(50)) > 0 ?
                    (bidder - NexmarkEventDeserializationSchema.random.nextInt(50)) : bidder;
            return new Bid(auction, newBidder , price, channel, url, extra);
        }

    }

    static public class NexmarkEventDeserializationSchema implements KafkaRecordDeserializationSchema<NexmarkEvent>{
        private static final ObjectMapper objectMapper = new ObjectMapper();
        int markerLoop = 0;
        final int markerInterval;
        final int copyInSource;
        static public Random random = new Random(42);

        public NexmarkEventDeserializationSchema(int markerInterval, int copyInSource) {
            this.markerInterval = markerInterval;
            this.copyInSource = copyInSource;
        }

        @Override
        public TypeInformation<NexmarkEvent> getProducedType() {
            return TypeExtractor.getForClass(NexmarkEvent.class);
        }

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record,
                Collector<NexmarkEvent> out) throws IOException {
            if (record.value() != null) {
                try {
                    JsonNode rootNode = objectMapper.readTree(record.value());
                    if (rootNode.has("type") && rootNode.get("type").asText().equals("init")) {
                        return;
                    }
                    long creationTime = rootNode.get("timestamp").asLong();
                    creationTime *= 1000; // python timestamp is in milliseconds, so we need to convert it to microseconds
                    NexmarkEvent event;
                    switch (rootNode.get("event_type").asInt()) {
                        case Person.EVENT_ID:
                            event = objectMapper.treeToValue(rootNode.get("event"), Person.class);
                            break;
                        case Auction.EVENT_ID:
                            event = objectMapper.treeToValue(rootNode.get("event"), Auction.class);
                            break;
                        case Bid.EVENT_ID:
                            event = objectMapper.treeToValue(rootNode.get("event"), Bid.class);
                            break;
                        default:
                            throw new IOException("Unknown event type: " + rootNode.get("eventType").asInt());
                    }
                    event.creationTime = creationTime; // python timestamp is in milliseconds, so we need to convert it to microseconds
                    out.collect(event);
                    // add more events to increase the throughput
                    for (int i = 0; i < copyInSource; i++) {
                        out.collect(event.perturbClone());
                    }

                    markerLoop++;
                    if (markerLoop == markerInterval) {
                        out.collect(new Marker(creationTime));
                        markerLoop = 0;
                    }
                } catch (Exception e) {
                    throw new IOException("Failed to deserialize record", e);
                }
            }
        }
    }
}
