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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.state.KeyOrStateID;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.Serializable;
import java.util.List;

/**
 * An entity keeping all the time-related services.
 *
 * <p><b>NOTE:</b> These services are only available to keyed operators.
 *
 * @param <K> The type of keys used for the timers and the registry.
 */
@Internal
public interface InternalTimeServiceManager<K> {
    /**
     * Creates an {@link InternalTimerService} for handling a group of timers identified by the
     * given {@code name}. The timers are scoped to a key and namespace.
     *
     * <p>When a timer fires the given {@link Triggerable} will be invoked.
     */
    <N> InternalTimerService<N> getInternalTimerService(
            String name,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            Triggerable<K, N> triggerable);

    /**
     * Advances the Watermark of all managed {@link InternalTimerService timer services},
     * potentially firing event time timers.
     */
    void advanceWatermark(Watermark watermark) throws Exception;

    /**
     * Snapshots the timers to raw keyed state.
     *
     * <p><b>TODO:</b> This can be removed once heap-based timers are integrated with RocksDB
     * incremental snapshots.
     */
    void snapshotToRawKeyedState(
            KeyedStateCheckpointOutputStream stateCheckpointOutputStream, String operatorName)
            throws Exception;

    /**
     * A provider pattern for creating an instance of a {@link InternalTimeServiceManager}. Allows
     * substituting the manager that will be used at the runtime.
     */
    @FunctionalInterface
    interface Provider extends Serializable {
        <K> InternalTimeServiceManager<K> create(
                CheckpointableKeyedStateBackend<K> keyedStatedBackend,
                ClassLoader userClassloader,
                KeyContext keyContext,
                ProcessingTimeService processingTimeService,
                Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates)
                throws Exception;
    }

    //--------------------  scale utils --------------------
    default void updateKeyGroupRange(KeyGroupRange keyGroupRange){
        throw new UnsupportedOperationException("Not support updateKeyGroupRange in " + this.getClass().getSimpleName());
    }
    default void collectRawKeyedState(KeyOrStateID keyOrStateID, ScaleBuffer.StateBuffer migratingState, ByteBuf byteBuf){
        throw new UnsupportedOperationException("Not support collectRawKeyedState in " + this.getClass().getSimpleName());
    }
    default void updateTimeServiceForStateMerge(ScaleBuffer.StateBuffer stateBuffer){
        throw new UnsupportedOperationException("Not support updateTimeServiceForStateMerge in " + this.getClass().getSimpleName());
    }
}
