package org.apache.flink.runtime.scale.rest;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * HTTP handlers for asynchronous triggering of scale.
 */
public class JobScaleHandlers {
    protected static final Logger LOG = LoggerFactory.getLogger(JobScaleHandlers.class);

    private abstract static class JobScaleHandlerBase<B extends RequestBody>
            extends AbstractRestHandler<
                    RestfulGateway, B, TriggerResponse, ScaleTriggerMessageParameters>{
        JobScaleHandlerBase(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                final MessageHeaders<B, TriggerResponse, ScaleTriggerMessageParameters>
                        messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        protected AsynchronousJobOperationKey createOperationKey(final HandlerRequest<B> request) {
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return AsynchronousJobOperationKey.of(
                    extractTriggerId(request.getRequestBody()).orElseGet(TriggerId::new), jobId);
        }

        protected abstract Optional<TriggerId> extractTriggerId(B request);

        public CompletableFuture<TriggerResponse> handleRequest(
                @Nonnull HandlerRequest<B> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            final AsynchronousJobOperationKey operationKey = createOperationKey(request);

            return triggerOperation(request, operationKey, gateway)
                    .handle(
                            (acknowledge, throwable) -> {
                                if (throwable == null) {
                                    return new TriggerResponse(operationKey.getTriggerId());
                                } else {
                                    LOG.error(
                                            "Failed to trigger operation with triggerId={} for job {}.",
                                            operationKey.getTriggerId(),
                                            operationKey.getJobId(),
                                            throwable);
                                    throw new CompletionException(
                                            createInternalServerError(
                                                    throwable, operationKey, "triggering"));
                                }
                            });
        }

        protected abstract CompletableFuture<Acknowledge> triggerOperation(
                HandlerRequest<B> request,
                AsynchronousJobOperationKey operationKey,
                RestfulGateway gateway)
                throws RestHandlerException;
    }

    public class ScaleTriggerHandler extends JobScaleHandlerBase<ScaleTriggerRequestBody>{
        public ScaleTriggerHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(leaderRetriever, timeout, responseHeaders, ScaleTriggerHeaders.getInstance());
        }

        @Override
        protected Optional<TriggerId> extractTriggerId(ScaleTriggerRequestBody requestBody) {
            return requestBody.getTriggerId();
        }

        @Override
        protected CompletableFuture<Acknowledge> triggerOperation(
                HandlerRequest<ScaleTriggerRequestBody> request,
                AsynchronousJobOperationKey operationKey,
                RestfulGateway gateway)
                throws RestHandlerException {

            String operatorName = request.getRequestBody().getOperatorName();
            int newParallelism = request.getRequestBody().getNewParallelism();
            LOG.info("Triggering scale operation for operator {} with new parallelism {}",
                    operatorName, newParallelism);

            return gateway.triggerScale(
                            operationKey, operatorName, newParallelism ,RpcUtils.INF_TIMEOUT)
                    .handle(
                            (Void ack, Throwable throwable) -> {
                                if (throwable != null) {
                                    throw new CompletionException(
                                            new RestHandlerException(
                                                    "Could not trigger scale operation.",
                                                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                    throwable)
                                    );
                                } else {
                                    return Acknowledge.get();
                                }
                            });
        }
    }

    public class ScaleStatusHandler
            extends AbstractRestHandler<
            RestfulGateway,
            EmptyRequestBody,
            AsynchronousOperationResult<ScaleMetricsInfo>,
            ScaleMessageParametersWithTriggerID>{
        public ScaleStatusHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(leaderRetriever, timeout, responseHeaders, ScaleStatusHeaders.getInstance());
        }

        @Override
        public CompletableFuture<AsynchronousOperationResult<ScaleMetricsInfo>> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
            return gateway.getScaleStatus(jobId, triggerId)
                    .handle(
                            (status, throwable) -> {
                                if (throwable != null) {
                                    LOG.error(
                                            "Failed to get scale status for job {} with triggerId={}.",
                                            jobId,
                                            triggerId,
                                            throwable);
                                    throw new CompletionException(
                                            createInternalServerError(
                                                    throwable, AsynchronousJobOperationKey.of(triggerId, jobId), "getting"));
                                } else {
                                    if (status == null) {
                                        return AsynchronousOperationResult.inProgress();
                                    }
                                    return AsynchronousOperationResult.completed(status);
                                }
                            });
        }
    }

    public class StateSizeHandler extends AbstractRestHandler<
            RestfulGateway,
            EmptyRequestBody,
            AsynchronousOperationResult<StateSizeInfo>,
            StateSizeMessageParameters>{
        public StateSizeHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(leaderRetriever, timeout, responseHeaders, StateSizeHeaders.getInstance());
        }
        @Override
        public CompletableFuture<AsynchronousOperationResult<StateSizeInfo>> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            final JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
            return gateway.getStateSize(jobId, jobVertexID)
                    .handle(
                            (size, throwable) -> {
                                if (throwable != null) {
                                    throw new CompletionException(
                                            createInternalServerError(
                                                    throwable, AsynchronousJobOperationKey.of(new TriggerId(), jobId), "getting"));
                                }else{
                                    if (size == null) {
                                        return AsynchronousOperationResult.inProgress();
                                    }
                                    return AsynchronousOperationResult.completed(new StateSizeInfo(size));
                                }
                            });
        }
    }

    public class SubscaleTriggerHandler extends AbstractRestHandler<
            RestfulGateway, SubscaleTriggerRequestBody, TriggerResponse, ScaleMessageParametersWithTriggerID>{

        public SubscaleTriggerHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(leaderRetriever, timeout, responseHeaders, SubscaleTriggerHeaders.getInstance());
        }

        @Override
        public CompletableFuture<TriggerResponse> handleRequest(
                @Nonnull HandlerRequest<SubscaleTriggerRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {

            final List<Integer> keys = request.getRequestBody().getKeys();
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);

            return gateway.triggerSubscale(jobId, triggerId, keys, RpcUtils.INF_TIMEOUT)
                    .handle(
                            (acknowledge, throwable) -> {
                                if (throwable == null) {
                                    return new TriggerResponse(triggerId);
                                } else {
                                    LOG.error(
                                            "Failed to trigger operation with triggerId={} for job {}.",
                                            triggerId,
                                            jobId,
                                            throwable);
                                    throw new CompletionException(
                                            createInternalServerError(
                                                    throwable, AsynchronousJobOperationKey.of(triggerId, jobId),
                                                            "triggering"));
                                }
                            });
        }
    }



    private static RestHandlerException createInternalServerError(
            Throwable throwable, AsynchronousJobOperationKey key, String errorMessageInfix) {
        return new RestHandlerException(
                String.format(
                        "Internal server error while %s scale operation with triggerId=%s for job %s.",
                        errorMessageInfix, key.getTriggerId(), key.getJobId()),
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                throwable);
    }



}
