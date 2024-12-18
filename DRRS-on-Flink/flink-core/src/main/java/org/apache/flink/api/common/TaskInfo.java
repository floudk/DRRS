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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Internal;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Encapsulates task-specific information: name, index of subtask, parallelism and attempt number.
 */
@Internal
public class TaskInfo {

    private final String taskName;
    private String taskNameWithSubtasks;
    private final String allocationIDAsString;
    private final int maxNumberOfParallelSubtasks;
    private final int indexOfSubtask;
    private int numberOfParallelSubtasks;
    private final int attemptNumber;

    public TaskInfo(
            String taskName,
            int maxNumberOfParallelSubtasks,
            int indexOfSubtask,
            int numberOfParallelSubtasks,
            int attemptNumber) {
        this(
                taskName,
                maxNumberOfParallelSubtasks,
                indexOfSubtask,
                numberOfParallelSubtasks,
                attemptNumber,
                "UNKNOWN");
    }

    public TaskInfo(
            String taskName,
            int maxNumberOfParallelSubtasks,
            int indexOfSubtask,
            int numberOfParallelSubtasks,
            int attemptNumber,
            String allocationIDAsString) {

        checkArgument(indexOfSubtask >= 0, "Task index must be a non-negative number.");
        checkArgument(
                maxNumberOfParallelSubtasks >= 1, "Max parallelism must be a positive number.");
        checkArgument(
                maxNumberOfParallelSubtasks >= numberOfParallelSubtasks,
                "Max parallelism must be >= than parallelism.");
        checkArgument(numberOfParallelSubtasks >= 1, "Parallelism must be a positive number.");
        checkArgument(
                indexOfSubtask < numberOfParallelSubtasks,
                "Task index must be less than parallelism.");
        checkArgument(attemptNumber >= 0, "Attempt number must be a non-negative number.");
        this.taskName = checkNotNull(taskName, "Task Name must not be null.");
        this.maxNumberOfParallelSubtasks = maxNumberOfParallelSubtasks;
        this.indexOfSubtask = indexOfSubtask;
        this.numberOfParallelSubtasks = numberOfParallelSubtasks;
        this.attemptNumber = attemptNumber;
        this.taskNameWithSubtasks =
                taskName
                        + " ("
                        + (indexOfSubtask + 1)
                        + '/'
                        + numberOfParallelSubtasks
                        + ')'
                        + "#"
                        + attemptNumber;
        this.allocationIDAsString = checkNotNull(allocationIDAsString);
    }

    /**
     * Returns the name of the task
     *
     * @return The name of the task
     */
    public String getTaskName() {
        return this.taskName;
    }

    /** Gets the max parallelism aka the max number of subtasks. */
    public int getMaxNumberOfParallelSubtasks() {
        return maxNumberOfParallelSubtasks;
    }

    /**
     * Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
     * parallelism-1 (parallelism as returned by {@link #getNumberOfParallelSubtasks()}).
     *
     * @return The index of the parallel subtask.
     */
    public int getIndexOfThisSubtask() {
        return this.indexOfSubtask;
    }

    /**
     * Gets the parallelism with which the parallel task runs.
     *
     * @return The parallelism with which the parallel task runs.
     */
    public int getNumberOfParallelSubtasks() {
        return this.numberOfParallelSubtasks;
    }

    /**
     * Gets the attempt number of this parallel subtask. First attempt is numbered 0. The attempt
     * number corresponds to the number of times this task has been restarted(after
     * failure/cancellation) since the job was initially started.
     *
     * @return Attempt number of the subtask.
     */
    public int getAttemptNumber() {
        return this.attemptNumber;
    }

    /**
     * Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)#1",
     * where 3 would be ({@link #getIndexOfThisSubtask()} + 1), and 6 would be {@link
     * #getNumberOfParallelSubtasks()}, and 1 would be {@link #getAttemptNumber()}.
     *
     * @return The name of the task, with subtask indicator.
     */
    public String getTaskNameWithSubtasks() {
        return this.taskNameWithSubtasks;
    }

    /**
     * Returns the allocation id for where this task is executed.
     *
     * @return the allocation id for where this task is executed.
     */
    public String getAllocationIDAsString() {
        return allocationIDAsString;
    }

    // ------------------------------------------------------------------------
    // Scale Utils
    // ------------------------------------------------------------------------
    /**
     * Update the TaskInfo with the new parallelism.
     */
    public void scale(int newParallelism) {
        this.numberOfParallelSubtasks = newParallelism;
        this.taskNameWithSubtasks =
                taskName
                        + " ("
                        + (indexOfSubtask + 1)
                        + '/'
                        + numberOfParallelSubtasks
                        + ')'
                        + "#"
                        + attemptNumber;
    }
}
