package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class ScaleStatusHeaders extends AsynchronousOperationStatusMessageHeaders<
        ScaleMetricsInfo, ScaleMessageParametersWithTriggerID> {

    private static final ScaleStatusHeaders INSTANCE = new ScaleStatusHeaders();

    private static final String URL =
            String.format(
                    "/jobs/:%s/scale/:%s", JobIDPathParameter.KEY, TriggerIdPathParameter.KEY);

    private ScaleStatusHeaders() {}

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public ScaleMessageParametersWithTriggerID getUnresolvedMessageParameters() {
        return new ScaleMessageParametersWithTriggerID();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static ScaleStatusHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<ScaleMetricsInfo> getValueClass() {
        return ScaleMetricsInfo.class;
    }

    @Override
    public String getDescription() {
        return "Returns the status of a scale operation.";
    }
}
