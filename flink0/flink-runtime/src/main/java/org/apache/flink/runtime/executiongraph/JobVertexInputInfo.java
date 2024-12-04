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

package org.apache.flink.runtime.executiongraph;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class describe how a job vertex consume an input(intermediate result). */
public class JobVertexInputInfo {

    private final List<ExecutionVertexInputInfo> executionVertexInputInfos;

    public JobVertexInputInfo(final List<ExecutionVertexInputInfo> executionVertexInputInfos) {
        this.executionVertexInputInfos = checkNotNull(executionVertexInputInfos);
    }

    /** The input information of subtasks of this job vertex. */
    public List<ExecutionVertexInputInfo> getExecutionVertexInputInfos() {
        return executionVertexInputInfos;
    }

    // ---------------------------------------------------------------------------
    // Scale Utils
    // ---------------------------------------------------------------------------

    public ExecutionVertexInputInfo getExecutionVertexInputInfo(int subtaskIndex) {

        checkArgument(subtaskIndex>=0 && subtaskIndex<executionVertexInputInfos.size(),
                "try to get input info of subtask " +
                        subtaskIndex +
                        " but the size is " +
                        executionVertexInputInfos.size());

        ExecutionVertexInputInfo executionVertexInputInfo =
                executionVertexInputInfos.get(subtaskIndex);
        checkArgument(subtaskIndex == executionVertexInputInfo.getSubtaskIndex(),
                "The subtask index of the input info is not match"
                        +subtaskIndex+" "+executionVertexInputInfo.getSubtaskIndex());
        return executionVertexInputInfo;
    }
}