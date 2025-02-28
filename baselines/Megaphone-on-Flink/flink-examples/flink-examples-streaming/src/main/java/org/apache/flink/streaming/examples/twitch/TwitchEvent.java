package org.apache.flink.streaming.examples.twitch;

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

public class TwitchEvent extends ScaleEvaEvent {
    public int processingTask = -1;

    @Override
    public void setSubtaskIndex(int subtaskIndex) {
        this.processingTask = subtaskIndex;
    }

    public enum EventType {ENTER, LEAVE}

    public String userId;
    public String streamerName;

    @JsonProperty("eventType")
    public EventType eventType;

    @JsonCreator
    public TwitchEvent(
            @JsonProperty("userId") String userId,
            @JsonProperty("streamerName") String streamerName,
            @JsonProperty("eventTime") long creationTime,
            @JsonProperty("eventType") EventType eventType) {
        this.userId = userId;
        this.streamerName = streamerName;
        this.creationTime = creationTime;
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "TwitchEvent{"
                + userId + ", " + streamerName + ", " + creationTime + ", " + eventType + "}";
    }

    static class Marker extends TwitchEvent {
        public Marker(long creationTime) {
            super("", "", creationTime, EventType.ENTER);
            isMarker = true;
        }
    }

    static public class TwitchEventDeserializationSchema implements KafkaRecordDeserializationSchema<TwitchEvent> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        int markerLoop = 0;
        final int markerInterval;
        public TwitchEventDeserializationSchema(int markerInterval) {
            this.markerInterval = markerInterval;
        }

        @Override
        public TypeInformation<TwitchEvent> getProducedType() {
            return TypeExtractor.getForClass(TwitchEvent.class);
        }

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record,
                Collector<TwitchEvent> out) throws IOException {
            if (record.value() != null) {
                try {
                    JsonNode rootNode = objectMapper.readTree(record.value());
                    if (rootNode.has("type") && rootNode.get("type").asText().equals("init")) {
                        return;
                    }
                    TwitchEvent event = objectMapper.treeToValue(rootNode, TwitchEvent.class);
                    out.collect(event);

                    markerLoop++;
                    if (markerLoop == markerInterval) {
                        out.collect(new Marker(event.creationTime));
                        markerLoop = 0;
                    }
                } catch (Exception e) {
                    throw new IOException("Failed to deserialize record", e);
                }
            }
        }
    }
}
