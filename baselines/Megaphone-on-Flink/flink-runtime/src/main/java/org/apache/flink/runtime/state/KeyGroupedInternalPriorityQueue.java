/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface exists as (temporary) adapter between the new {@link InternalPriorityQueue} and
 * the old way in which timers are written in a snapshot. This interface can probably go away once
 * timer state becomes part of the keyed state backend snapshot.
 */
public interface KeyGroupedInternalPriorityQueue<T> extends InternalPriorityQueue<T> {

    /**
     * Returns the subset of elements in the priority queue that belongs to the given key-group,
     * within the operator's key-group range.
     */
    @Nonnull
    Set<T> getSubsetForKeyGroup(int keyGroupId);

    @Nonnull
    default Map<Integer,Set<T>> getAndRemoveSubsetForKeyGroup(List<Integer> keyGroups) {
        throw new UnsupportedOperationException("This priority queue(" + getClass().getName() + ") does not support removal of elements.");
    }

    default void updateKeyGroupRange(KeyGroupRange keyGroupRange) {
        throw new UnsupportedOperationException("This priority queue(" + getClass().getName() + ") does not support update of key group range.");
    }

    default void mergeKeyGroup(int mergedKeyGroup){
        throw new UnsupportedOperationException("This priority queue(" + getClass().getName() + ") does not support merge of key group.");
    }
}
