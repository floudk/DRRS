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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.scale.coordinator.SubtaskScaleCoordinator;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.util.UserCodeClassLoader;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** In implementation of the {@link Environment}. */
public class RuntimeEnvironment implements Environment {

    private final JobID jobId;
    private final JobVertexID jobVertexId;
    private final ExecutionAttemptID executionId;

    private final TaskInfo taskInfo;

    private final Configuration jobConfiguration;
    private final Configuration taskConfiguration;
    private final ExecutionConfig executionConfig;

    private final UserCodeClassLoader userCodeClassLoader;

    private final MemoryManager memManager;
    private final SharedResources sharedResources;
    private final IOManager ioManager;
    private final BroadcastVariableManager bcVarManager;
    private final TaskStateManager taskStateManager;
    private final GlobalAggregateManager aggregateManager;
    private final InputSplitProvider splitProvider;
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    private final Map<String, Future<Path>> distCacheEntries;

    private final ResultPartitionWriter[] writers;
    private final IndexedInputGate[] inputGates;

    private final TaskEventDispatcher taskEventDispatcher;

    private final CheckpointResponder checkpointResponder;
    private final TaskOperatorEventGateway operatorEventGateway;

    private final AccumulatorRegistry accumulatorRegistry;

    private final TaskKvStateRegistry kvStateRegistry;

    private final TaskManagerRuntimeInfo taskManagerInfo;
    private final TaskMetricGroup metrics;

    private final Task containingTask;

    @Nullable private MailboxExecutor mainMailboxExecutor;

    @Nullable private ExecutorService asyncOperationsThreadPool;

    @Nullable private CheckpointStorageAccess checkpointStorageAccess;

    ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory;

    SubtaskScaleCoordinator subtaskScaleCoordinator;

    // ------------------------------------------------------------------------

    public RuntimeEnvironment(
            JobID jobId,
            JobVertexID jobVertexId,
            ExecutionAttemptID executionId,
            ExecutionConfig executionConfig,
            TaskInfo taskInfo,
            Configuration jobConfiguration,
            Configuration taskConfiguration,
            UserCodeClassLoader userCodeClassLoader,
            MemoryManager memManager,
            SharedResources sharedResources,
            IOManager ioManager,
            BroadcastVariableManager bcVarManager,
            TaskStateManager taskStateManager,
            GlobalAggregateManager aggregateManager,
            AccumulatorRegistry accumulatorRegistry,
            TaskKvStateRegistry kvStateRegistry,
            InputSplitProvider splitProvider,
            Map<String, Future<Path>> distCacheEntries,
            ResultPartitionWriter[] writers,
            IndexedInputGate[] inputGates,
            TaskEventDispatcher taskEventDispatcher,
            CheckpointResponder checkpointResponder,
            TaskOperatorEventGateway operatorEventGateway,
            TaskManagerRuntimeInfo taskManagerInfo,
            TaskMetricGroup metrics,
            Task containingTask,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory,
            SubtaskScaleCoordinator subtaskScaleCoordinator) {

        this.jobId = checkNotNull(jobId);
        this.jobVertexId = checkNotNull(jobVertexId);
        this.executionId = checkNotNull(executionId);
        this.taskInfo = checkNotNull(taskInfo);
        this.executionConfig = checkNotNull(executionConfig);
        this.jobConfiguration = checkNotNull(jobConfiguration);
        this.taskConfiguration = checkNotNull(taskConfiguration);
        this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
        this.memManager = checkNotNull(memManager);
        this.sharedResources = checkNotNull(sharedResources);
        this.ioManager = checkNotNull(ioManager);
        this.bcVarManager = checkNotNull(bcVarManager);
        this.taskStateManager = checkNotNull(taskStateManager);
        this.aggregateManager = checkNotNull(aggregateManager);
        this.accumulatorRegistry = checkNotNull(accumulatorRegistry);
        this.kvStateRegistry = checkNotNull(kvStateRegistry);
        this.splitProvider = checkNotNull(splitProvider);
        this.distCacheEntries = checkNotNull(distCacheEntries);
        this.writers = checkNotNull(writers);
        this.inputGates = checkNotNull(inputGates);
        this.taskEventDispatcher = checkNotNull(taskEventDispatcher);
        this.checkpointResponder = checkNotNull(checkpointResponder);
        this.operatorEventGateway = checkNotNull(operatorEventGateway);
        this.taskManagerInfo = checkNotNull(taskManagerInfo);
        this.containingTask = containingTask;
        this.metrics = metrics;
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);
        this.channelStateExecutorFactory = checkNotNull(channelStateExecutorFactory);

        this.subtaskScaleCoordinator = checkNotNull(subtaskScaleCoordinator);
    }

    // ------------------------------------------------------------------------

    @Override
    public ExecutionConfig getExecutionConfig() {
        return this.executionConfig;
    }

    @Override
    public JobID getJobID() {
        return jobId;
    }

    @Override
    public JobVertexID getJobVertexId() {
        return jobVertexId;
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionId;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return this.taskInfo;
    }

    @Override
    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    @Override
    public Configuration getTaskConfiguration() {
        return taskConfiguration;
    }

    @Override
    public TaskManagerRuntimeInfo getTaskManagerInfo() {
        return taskManagerInfo;
    }

    @Override
    public TaskMetricGroup getMetricGroup() {
        return metrics;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memManager;
    }

    @Override
    public SharedResources getSharedResources() {
        return sharedResources;
    }

    @Override
    public IOManager getIOManager() {
        return ioManager;
    }

    @Override
    public BroadcastVariableManager getBroadcastVariableManager() {
        return bcVarManager;
    }

    @Override
    public TaskStateManager getTaskStateManager() {
        return taskStateManager;
    }

    @Override
    public GlobalAggregateManager getGlobalAggregateManager() {
        return aggregateManager;
    }

    @Override
    public AccumulatorRegistry getAccumulatorRegistry() {
        return accumulatorRegistry;
    }

    @Override
    public TaskKvStateRegistry getTaskKvStateRegistry() {
        return kvStateRegistry;
    }

    @Override
    public InputSplitProvider getInputSplitProvider() {
        return splitProvider;
    }

    @Override
    public Map<String, Future<Path>> getDistributedCacheEntries() {
        return distCacheEntries;
    }

    @Override
    public ResultPartitionWriter getWriter(int index) {
        return writers[index];
    }

    @Override
    public ResultPartitionWriter[] getAllWriters() {
        return writers;
    }

    @Override
    public IndexedInputGate getInputGate(int index) {
        return inputGates[index];
    }

    @Override
    public IndexedInputGate[] getAllInputGates() {
        return inputGates;
    }

    @Override
    public TaskEventDispatcher getTaskEventDispatcher() {
        return taskEventDispatcher;
    }

    @Override
    public ExternalResourceInfoProvider getExternalResourceInfoProvider() {
        return externalResourceInfoProvider;
    }

    @Override
    public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {
        acknowledgeCheckpoint(checkpointId, checkpointMetrics, null);
    }

    @Override
    public void acknowledgeCheckpoint(
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointStateHandles) {

        checkpointResponder.acknowledgeCheckpoint(
                jobId, executionId, checkpointId, checkpointMetrics, checkpointStateHandles);
    }

    @Override
    public void declineCheckpoint(long checkpointId, CheckpointException checkpointException) {
        checkpointResponder.declineCheckpoint(
                jobId, executionId, checkpointId, checkpointException);
    }

    @Override
    public TaskOperatorEventGateway getOperatorCoordinatorEventGateway() {
        return operatorEventGateway;
    }

    @Override
    public void failExternally(Throwable cause) {
        this.containingTask.failExternally(cause);
    }

    @Override
    public void setMainMailboxExecutor(MailboxExecutor mainMailboxExecutor) {
        checkState(this.mainMailboxExecutor == null, "Can not set mainMailboxExecutor twice!");
        this.mainMailboxExecutor = mainMailboxExecutor;
    }

    @Override
    public MailboxExecutor getMainMailboxExecutor() {
        return checkNotNull(
                mainMailboxExecutor, "mainMailboxExecutor has not been initialized yet!");
    }

    @Override
    public void setAsyncOperationsThreadPool(ExecutorService executorService) {
        checkState(
                this.asyncOperationsThreadPool == null,
                "Can not set asyncOperationsThreadPool twice!");
        this.asyncOperationsThreadPool = executorService;
    }

    @Override
    public ExecutorService getAsyncOperationsThreadPool() {
        return checkNotNull(
                asyncOperationsThreadPool,
                "asyncOperationsThreadPool has not been initialized yet!");
    }

    @Override
    public void setCheckpointStorageAccess(CheckpointStorageAccess checkpointStorageAccess) {
        checkState(
                this.checkpointStorageAccess == null, "Can not set checkpointStorageAccess twice!");
        this.checkpointStorageAccess = checkpointStorageAccess;
    }

    @Override
    public CheckpointStorageAccess getCheckpointStorageAccess() {
        return checkNotNull(
                checkpointStorageAccess, "checkpointStorage has not been initialized yet!");
    }

    @Override
    public ChannelStateWriteRequestExecutorFactory getChannelStateExecutorFactory() {
        return channelStateExecutorFactory;
    }

    // ------------------------------------------------------------------------
    // Scale Utils
    // ------------------------------------------------------------------------
    @Override
    public SubtaskScaleCoordinator getSubtaskScaleCoordinator() {
        return subtaskScaleCoordinator;
    }
}
