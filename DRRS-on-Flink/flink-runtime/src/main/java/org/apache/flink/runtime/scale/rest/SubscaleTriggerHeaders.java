package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class SubscaleTriggerHeaders extends AsynchronousOperationTriggerMessageHeaders<
        SubscaleTriggerRequestBody, ScaleMessageParametersWithTriggerID> {

    private static final SubscaleTriggerHeaders INSTANCE = new SubscaleTriggerHeaders();

    private static final String URL =
            String.format(
                    "/jobs/:%s/scale/:%s/subscale", JobIDPathParameter.KEY, TriggerIdPathParameter.KEY);

    private SubscaleTriggerHeaders() {}

    @Override
    public Class<SubscaleTriggerRequestBody> getRequestClass() {
        return SubscaleTriggerRequestBody.class;
    }

    @Override
    public ScaleMessageParametersWithTriggerID getUnresolvedMessageParameters() {
        return new ScaleMessageParametersWithTriggerID();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static SubscaleTriggerHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    protected String getAsyncOperationDescription() {
        return "Trigger subscale after scale initiated";
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.ACCEPTED;
    }

    @Override
    public String operationId() {
        return "triggerSubscale";
    }
}
