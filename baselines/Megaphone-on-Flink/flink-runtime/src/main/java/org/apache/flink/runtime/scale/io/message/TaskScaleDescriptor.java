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
 * limitations under the License
 */

package org.apache.flink.runtime.scale.io.message;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.scale.coordinator.ScaleContext;

import java.io.Serializable;
import java.util.List;

/**
 * Trigger Barrier to notify the task to initialize and start the scale operation.
 */
public class TaskScaleDescriptor implements Serializable {
    private static final long serialVersionUID = 14L;

    private final List<ConnectionID> allConnectionIds;
    private final Integer newParallelism;

    public TaskScaleDescriptor(ScaleContext context){
        this.allConnectionIds = context.getConnectionIDS();
        this.newParallelism = context.getNewParallelism();
    }
    public List<ConnectionID> getAllConnectionIds() {
        return allConnectionIds;
    }
    public int getNewParallelism() {
        return newParallelism;
    }
}
