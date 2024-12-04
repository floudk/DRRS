package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class ScaleTriggerHeaders
        extends AsynchronousOperationTriggerMessageHeaders<
                        ScaleTriggerRequestBody, ScaleTriggerMessageParameters> {

    private static final ScaleTriggerHeaders INSTANCE = new ScaleTriggerHeaders();

    private static final String URL = String.format("/jobs/:%s/scale", JobIDPathParameter.KEY);

    private ScaleTriggerHeaders() {}

    public static ScaleTriggerHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<ScaleTriggerRequestBody> getRequestClass(){
        return ScaleTriggerRequestBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.ACCEPTED;
    }

    @Override
    public ScaleTriggerMessageParameters getUnresolvedMessageParameters() {
        return new ScaleTriggerMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    protected String getAsyncOperationDescription() {
        return "Triggers a scale, and multiple subscales afterwards.";
    }

    @Override
    public String operationId() {
        return "triggerScale";
    }
}
