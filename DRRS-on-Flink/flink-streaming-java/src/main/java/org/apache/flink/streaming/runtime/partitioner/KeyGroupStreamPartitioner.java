/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.scale.io.KeyGroupRecordTracker;
import org.apache.flink.runtime.scale.io.SubscaleTriggerInfo;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Partitioner selects the target channel based on the key group index.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
@Internal
public class KeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T>
        implements ConfigurableStreamPartitioner {

    private static final Logger LOG = LoggerFactory.getLogger(KeyGroupStreamPartitioner.class);

    private static final long serialVersionUID = 1L;

    private final KeySelector<T, K> keySelector;

    private int maxParallelism;

    /**
     * By default, Flink do not use an explicit routing table to determine the target channel in this key-group partitioner.
     * but instead uses implicit rules to determine the target channel.
     * <p>
     * However, when the scale operation occurs, we need this to route the key more flexibly.
     */
    private int[] routingTable = null;
    // will be set to true after any scale operation, and keep true throughout the whole life cycle
    private Boolean usingRoutingTable = false;

    private transient KeyGroupRecordTracker keyGroupRecordTracker = null;

    public KeyGroupStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism) {
        Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
        this.keySelector = checkNotNull(keySelector);
        this.maxParallelism = maxParallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        K key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not extract key from " + record.getInstance().getValue(), e);
        }

        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
        keyGroupRecordTracker.updateInputRecordMetrics(keyGroup);

        if(usingRoutingTable){
            checkNotNull(routingTable,
                    "No routing table found in " + getClass().getSimpleName());
            try{
                return KeyGroupRangeAssignment.assignKeyToParallelOperatorWithRoutingTable(
                        key, maxParallelism, routingTable);
            } catch (Exception e){
                throw new RuntimeException(
                        "Could not extract key from " + Arrays.toString(routingTable), e);
            }
        }else{
            return KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    key, maxParallelism, numberOfChannels);
        }


    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.RANGE;
    }

    @Override
    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

//    @Override
//    public String toString() {
//        return "HASH";
//    }

    @Override
    public void configure(int maxParallelism) {
        KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
        this.maxParallelism = maxParallelism;
        keyGroupRecordTracker = new KeyGroupRecordTracker(maxParallelism,true );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final KeyGroupStreamPartitioner<?, ?> that = (KeyGroupStreamPartitioner<?, ?>) o;
        return maxParallelism == that.maxParallelism && keySelector.equals(that.keySelector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keySelector, maxParallelism);
    }

    // ------------------------------------------------------------------------
    // Scale Utils
    // ------------------------------------------------------------------------

    @Override
    public void expand(int newNumberOfChannels) {
        if (!usingRoutingTable) {
            usingRoutingTable = true;
            // initial routing table based on current key partitions
            routingTable = new int[maxParallelism];
            for (int i = 0; i < maxParallelism; i++) {
                routingTable[i] =
                        KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                maxParallelism, numberOfChannels, i);
            }
        }
        setup(newNumberOfChannels);
    }

    @Override
    public Map<Integer,Integer> getAndUpdateRoutingTable(Map<Integer, SubscaleTriggerInfo> newKeyPartitions){
        checkState(usingRoutingTable,
                "No routing table found in " + getClass().getSimpleName());
        // return unmodifiable map
        Map<Integer,Integer> result = new HashMap<>();

        // update routing table
        for (Map.Entry<Integer, SubscaleTriggerInfo> entry : newKeyPartitions.entrySet()) {
            result.put(entry.getKey(), routingTable[entry.getKey()]);
            routingTable[entry.getKey()] = entry.getValue().newPartitioningPos;
        }
        return result;
    }

    @Override
    public Tuple2<Long[], Double[]> getKeyGroupStatistics(){
        return new Tuple2<>(
                keyGroupRecordTracker.getTotalCounts(),
                keyGroupRecordTracker.getRates()
        );
    }


    @Override
    public String toString(){
        return "KeyGroupStreamPartitioner[usingRoutingTable: "
                + usingRoutingTable
                +"]";
    }
}
