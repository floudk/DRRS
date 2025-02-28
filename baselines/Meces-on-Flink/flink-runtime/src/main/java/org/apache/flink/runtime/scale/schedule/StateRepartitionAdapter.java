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

package org.apache.flink.runtime.scale.schedule;

import org.apache.flink.runtime.scale.coordinator.ScaleCoordinator;
import org.apache.flink.runtime.scale.coordinator.ScaleContext;

import org.apache.flink.runtime.scale.state.FlexibleKeyGroupRange;

import org.apache.flink.runtime.state.KeyGroupRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.createKeyGroupPartitions;

/**
 * 1. How to find the migrated state which has the lowest overhead or best effect ?
 * 2. How to find the aim task that has the lowest overhead or best effect ?
 * 3. If there is no available instance, how to place new task in current cluster ? (Optional)
 */

public abstract class StateRepartitionAdapter {

    protected static final Logger LOG = LoggerFactory.getLogger(ScaleCoordinator.class);

    public void calculateNewKeyPartitions(ScaleContext context){
        throw new RuntimeException("Should be implemented by subclass");
    }


    /**
     * Do no need to use collected current state distribution info to calculate info.
     */
    public static class DefaultStateRepartitionAdapter extends StateRepartitionAdapter {

        /**
         * For each upstream operator,
         * calculate the TaskStateScaleAssignment for tasks in each operator.
         * As what Flink originally does
         */
        @Override
        public void calculateNewKeyPartitions(ScaleContext context) {
            List<KeyGroupRange> keyGroupPartitions =
                    createKeyGroupPartitions(
                            context.getMaxParallelism(),
                            context.getNewParallelism());

            context.setNewKeyGroupPartitions(keyGroupPartitions.stream()
                    .map(FlexibleKeyGroupRange::fromKeyGroupRange)
                    .map(r->(FlexibleKeyGroupRange)r)
                    .collect(Collectors.toList()));
        }
    }

}
