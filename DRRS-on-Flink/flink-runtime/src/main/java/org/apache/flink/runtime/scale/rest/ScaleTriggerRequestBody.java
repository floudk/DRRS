package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Optional;

public class ScaleTriggerRequestBody implements RequestBody {

    private static final String FIELD_NAME_OPERATOR_NAME = "operator-name";
    private static final String FIELD_NAME_NEW_PARALLELISM = "new-parallelism";
    private static final String FIELD_NAME_TRIGGER_ID = "trigger-id";

    @JsonProperty(FIELD_NAME_OPERATOR_NAME)
    private final String operatorName;

    @JsonProperty(FIELD_NAME_NEW_PARALLELISM)
    private final int newParallelism;

    @JsonProperty(FIELD_NAME_TRIGGER_ID)
    @Nullable
    private final TriggerId triggerId;

    @JsonCreator
    public ScaleTriggerRequestBody(
            @JsonProperty(FIELD_NAME_OPERATOR_NAME) String operatorName,
            @JsonProperty(FIELD_NAME_NEW_PARALLELISM) int newParallelism,
            @Nullable @JsonProperty(FIELD_NAME_TRIGGER_ID) TriggerId triggerId) {
        this.operatorName = operatorName;
        this.newParallelism = newParallelism;
        this.triggerId = triggerId;
    }

    public int getNewParallelism() {
        return newParallelism;
    }
    public String getOperatorName() {
        return operatorName;
    }
    @JsonIgnore
    public Optional<TriggerId> getTriggerId() {
        return Optional.ofNullable(triggerId);
    }

}
