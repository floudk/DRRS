package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SubscaleTriggerRequestBody implements RequestBody {
    private static final String FIELD_NAME_SUBSCALE_KEYS = "keys";

    @JsonProperty(FIELD_NAME_SUBSCALE_KEYS)
    private final List<Integer> keys;
    public SubscaleTriggerRequestBody(@JsonProperty(FIELD_NAME_SUBSCALE_KEYS) List<Integer> keys) {
        this.keys = keys;
    }
    public List<Integer> getKeys() {
        return keys;
    }
}
