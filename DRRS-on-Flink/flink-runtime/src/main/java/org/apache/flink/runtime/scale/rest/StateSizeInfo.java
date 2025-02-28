package org.apache.flink.runtime.scale.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.flink.runtime.messages.webmonitor.InfoMessage;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import java.util.Map;

public class StateSizeInfo implements ResponseBody, InfoMessage {
    private static final long serialVersionUID = 1L;

    public static final String KEY_GROUP_STATE_SIZE = "keyGroupStateSize";
    // keyGroup -> stateSize
    @JsonProperty(KEY_GROUP_STATE_SIZE)
    private Map<Integer, Long> keyGroupStateSize;

    @JsonCreator
    public StateSizeInfo(@JsonProperty(KEY_GROUP_STATE_SIZE) Map<Integer, Long> keyGroupStateSize) {
        this.keyGroupStateSize = keyGroupStateSize;
    }

    @JsonProperty(KEY_GROUP_STATE_SIZE)
    public Map<Integer, Long> getKeyGroupStateSize() {
        return keyGroupStateSize;
    }

    @Override
    public String toString() {
        return "StateSizeInfo{" +
                keyGroupStateSize +
                '}';
    }

}
