package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;

import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class StateSizeHeaders extends AsynchronousOperationStatusMessageHeaders<
        StateSizeInfo, StateSizeMessageParameters> {

    public static final StateSizeHeaders INSTANCE = new StateSizeHeaders();

    private static final String URL =
            String.format(
                    "/jobs/:%s/scale/:%s/statesize", JobIDPathParameter.KEY, JobVertexIdPathParameter.KEY
            );

    private StateSizeHeaders() {}

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }
    @Override
    public StateSizeMessageParameters getUnresolvedMessageParameters() {
        return new StateSizeMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static StateSizeHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<StateSizeInfo> getValueClass() {
        return StateSizeInfo.class;
    }

    @Override
    public String getDescription() {
        return "Returns the state size of the job.";
    }


}
