package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.messages.webmonitor.InfoMessage;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class ScaleStatusInfo implements ResponseBody, InfoMessage {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_STATUS = "status";

    @JsonProperty(FIELD_NAME_STATUS)
    private final String status;

    @JsonCreator
    public ScaleStatusInfo(@JsonProperty(FIELD_NAME_STATUS) String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "ScaleStatusInfo{" + status + "}";
    }
}
