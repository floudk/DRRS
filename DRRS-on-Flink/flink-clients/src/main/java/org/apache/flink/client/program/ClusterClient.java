/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Encapsulates the functionality necessary to submit a program to a remote cluster.
 *
 * @param <T> type of the cluster id
 */
public interface ClusterClient<T> extends AutoCloseable {

    @Override
    void close();

    /**
     * Returns the cluster id identifying the cluster to which the client is connected.
     *
     * @return cluster id of the connected cluster
     */
    T getClusterId();

    /**
     * Return the Flink configuration object.
     *
     * @return The Flink configuration object
     */
    Configuration getFlinkConfiguration();

    /** Shut down the cluster that this client communicate with. */
    void shutDownCluster();

    /** Returns an URL (as a string) to the cluster web interface. */
    String getWebInterfaceURL();

    /**
     * Lists the currently running and finished jobs on the cluster.
     *
     * @return future collection of running and finished jobs
     * @throws Exception if no connection to the cluster could be established
     */
    CompletableFuture<Collection<JobStatusMessage>> listJobs() throws Exception;

    /**
     * Dispose the savepoint under the given path.
     *
     * @param savepointPath path to the savepoint to be disposed
     * @return acknowledge future of the dispose action
     */
    CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) throws FlinkException;

    /**
     * Submit the given {@link JobGraph} to the cluster.
     *
     * @param jobGraph to submit
     * @return {@link JobID} of the submitted job
     */
    CompletableFuture<JobID> submitJob(JobGraph jobGraph);

    /** Requests the {@link JobStatus} of the job with the given {@link JobID}. */
    CompletableFuture<JobStatus> getJobStatus(JobID jobId);

    /**
     * Request the {@link JobResult} for the given {@link JobID}.
     *
     * @param jobId for which to request the {@link JobResult}
     * @return Future which is completed with the {@link JobResult}
     */
    CompletableFuture<JobResult> requestJobResult(JobID jobId);

    /**
     * Requests and returns the accumulators for the given job identifier. Accumulators can be
     * requested while a is running or after it has finished. The default class loader is used to
     * deserialize the incoming accumulator results.
     *
     * @param jobID The job identifier of a job.
     * @return A Map containing the accumulator's name and its value.
     */
    default CompletableFuture<Map<String, Object>> getAccumulators(JobID jobID) {
        return getAccumulators(jobID, ClassLoader.getSystemClassLoader());
    }

    /**
     * Requests and returns the accumulators for the given job identifier. Accumulators can be
     * requested while a is running or after it has finished.
     *
     * @param jobID The job identifier of a job.
     * @param loader The class loader for deserializing the accumulator results.
     * @return A Map containing the accumulator's name and its value.
     */
    CompletableFuture<Map<String, Object>> getAccumulators(JobID jobID, ClassLoader loader);

    /**
     * Cancels a job identified by the job id.
     *
     * @param jobId the job id
     */
    CompletableFuture<Acknowledge> cancel(JobID jobId);

    /**
     * Cancels a job identified by the job id and triggers a savepoint.
     *
     * @param jobId the job id
     * @param savepointDirectory directory the savepoint should be written to
     * @param formatType a binary format of the savepoint
     * @return future of path where the savepoint is located
     */
    CompletableFuture<String> cancelWithSavepoint(
            JobID jobId, @Nullable String savepointDirectory, SavepointFormatType formatType);

    /**
     * Stops a program on Flink cluster whose job-manager is configured in this client's
     * configuration. Stopping works only for streaming programs. Be aware, that the program might
     * continue to run for a while after sending the stop command, because after sources stopped to
     * emit data all operators need to finish processing.
     *
     * @param jobId the job ID of the streaming program to stop
     * @param advanceToEndOfEventTime flag indicating if the source should inject a {@code
     *     MAX_WATERMARK} in the pipeline
     * @param savepointDirectory directory the savepoint should be written to
     * @param formatType a binary format of the savepoint
     * @return a {@link CompletableFuture} containing the path where the savepoint is located
     */
    CompletableFuture<String> stopWithSavepoint(
            final JobID jobId,
            final boolean advanceToEndOfEventTime,
            @Nullable final String savepointDirectory,
            final SavepointFormatType formatType);

    /**
     * Triggers a savepoint for the job identified by the job id. The savepoint will be written to
     * the given savepoint directory, or {@link
     * org.apache.flink.configuration.CheckpointingOptions#SAVEPOINT_DIRECTORY} if it is null.
     *
     * @param jobId job id
     * @param savepointDirectory directory the savepoint should be written to
     * @param formatType a binary format of the savepoint
     * @return path future where the savepoint is located
     */
    CompletableFuture<String> triggerSavepoint(
            JobID jobId, @Nullable String savepointDirectory, SavepointFormatType formatType);

    /**
     * Sends out a request to a specified coordinator and return the response.
     *
     * @param jobId specifies the job which the coordinator belongs to
     * @param operatorId specifies which coordinator to receive the request
     * @param request the request to send
     * @return the response from the coordinator
     */
    CompletableFuture<CoordinationResponse> sendCoordinationRequest(
            JobID jobId, OperatorID operatorId, CoordinationRequest request);

    /**
     * Return a set of ids of the completed cluster datasets.
     *
     * @return A set of ids of the completely cached intermediate dataset.
     */
    default CompletableFuture<Set<AbstractID>> listCompletedClusterDatasetIds() {
        return CompletableFuture.completedFuture(Collections.emptySet());
    }

    /**
     * Invalidate the cached intermediate dataset with the given id.
     *
     * @param clusterDatasetId id of the cluster dataset to be invalidated.
     * @return Future which will be completed when the cached dataset is invalidated.
     */
    default CompletableFuture<Void> invalidateClusterDataset(AbstractID clusterDatasetId) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * The client reports the heartbeat to the dispatcher for aliveness.
     *
     * @param jobId The jobId for the client and the job.
     * @return
     */
    default CompletableFuture<Void> reportHeartbeat(JobID jobId, long expiredTimestamp) {
        return FutureUtils.completedVoidFuture();
    }

    // ----------------------- Scale Utils ------------------------
    default CompletableFuture<String> triggerScale(
            JobID jobId,
            String operatorName,
            int newParallelism){
        throw new UnsupportedOperationException("This cluster client"
                + this.getClass().getName() + " does not support triggerScale");
    }
}
