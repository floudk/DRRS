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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.state.FlexibleKeyGroupRange;
import org.apache.flink.runtime.scale.state.HierarchicalStateID;
import org.apache.flink.runtime.scale.state.KeyOrStateID;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for state tables. Accesses to state are typically scoped by the currently active key,
 * as provided through the {@link InternalKeyContext}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class StateTable<K, N, S>
        implements StateSnapshotRestore, Iterable<StateEntry<K, N, S>> {
    private static final Logger LOG = LoggerFactory.getLogger(StateTable.class);

    protected final boolean enableHierarchicalState;

    /**
     * The key context view on the backend. This provides information, such as the currently active
     * key.
     */
    protected final InternalKeyContext<K> keyContext;

    /** Combined meta information such as name and serializers for this state. */
    protected RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo;

    /** The serializer of the key. */
    protected final TypeSerializer<K> keySerializer;

    /** The current key group range. */
    protected KeyGroupRange keyGroupRange;

    /**
     * Map for holding the actual state objects. The outer array represents the key-groups. All
     * array positions will be initialized with an empty state map.
     */
    protected StateMap<K, N, S>[] keyGroupedStateMaps;

    /**
     * @param keyContext the key context provides the key scope for all put/get/delete operations.
     * @param metaInfo the meta information, including the type serializer for state copy-on-write.
     * @param keySerializer the serializer of the key.
     */
    public StateTable(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer) {
        this(keyContext, metaInfo, keySerializer, false);
    }
    public StateTable(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer,
            boolean enableHierarchicalState) {
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.metaInfo = Preconditions.checkNotNull(metaInfo);
        this.keySerializer = Preconditions.checkNotNull(keySerializer);

        this.keyGroupRange = keyContext.getKeyGroupRange();

        this.enableHierarchicalState = enableHierarchicalState;
        if (enableHierarchicalState) {
           LOG.info("Enable hierarchical state");
        }

        @SuppressWarnings("unchecked")
        StateMap<K, N, S>[] state =
                (StateMap<K, N, S>[])
                        new StateMap[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
        this.keyGroupedStateMaps = state;
        for (int i = 0; i < this.keyGroupedStateMaps.length; i++) {
            this.keyGroupedStateMaps[i] = createStateMap();
        }

    }

    public abstract StateMap<K, N, S> createStateMap();

    @Override
    @Nonnull
    public abstract IterableStateSnapshot<K, N, S> stateSnapshot();

    // Main interface methods of StateTable -------------------------------------------------------

    /**
     * Returns whether this {@link StateTable} is empty.
     *
     * @return {@code true} if this {@link StateTable} has no elements, {@code false} otherwise.
     * @see #size()
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the total number of entries in this {@link StateTable}. This is the sum of both
     * sub-tables.
     *
     * @return the number of entries in this {@link StateTable}.
     */
    public int size() {
        int count = 0;
        for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
            count += stateMap.size();
        }
        return count;
    }

    /**
     * Returns the state of the mapping for the composite of active key and given namespace.
     *
     * @param namespace the namespace. Not null.
     * @return the states of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    public S get(N namespace) {
        return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Returns whether this table contains a mapping for the composite of active key and given
     * namespace.
     *
     * @param namespace the namespace in the composite key to search for. Not null.
     * @return {@code true} if this map contains the specified key/namespace composite key, {@code
     *     false} otherwise.
     */
    public boolean containsKey(N namespace) {
        return containsKey(
                keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Maps the composite of active key and given namespace to the specified state.
     *
     * @param namespace the namespace. Not null.
     * @param state the state. Can be null.
     */
    public void put(N namespace, S state) {
        put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
    }

    /**
     * Removes the mapping for the composite of active key and given namespace. This method should
     * be preferred over {@link #removeAndGetOld(N)} when the caller is not interested in the old
     * state.
     *
     * @param namespace the namespace of the mapping to remove. Not null.
     */
    public void remove(N namespace) {
        remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Removes the mapping for the composite of active key and given namespace, returning the state
     * that was found under the entry.
     *
     * @param namespace the namespace of the mapping to remove. Not null.
     * @return the state of the removed mapping or {@code null} if no mapping for the specified key
     *     was found.
     */
    public S removeAndGetOld(N namespace) {
        return removeAndGetOld(
                keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Applies the given {@link StateTransformationFunction} to the state (1st input argument),
     * using the given value as second input argument. The result of {@link
     * StateTransformationFunction#apply(Object, Object)} is then stored as the new state. This
     * function is basically an optimization for get-update-put pattern.
     *
     * @param namespace the namespace. Not null.
     * @param value the value to use in transforming the state. Can be null.
     * @param transformation the transformation function.
     * @throws Exception if some exception happens in the transformation function.
     */
    public <T> void transform(
            N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        K key = keyContext.getCurrentKey();
        checkKeyNamespacePreconditions(key, namespace);

        int keyGroup = keyContext.getCurrentKeyGroupIndex();
        getMapForKeyGroup(keyGroup).transform(key, namespace, value, transformation);
    }

    // For queryable state ------------------------------------------------------------------------

    /**
     * Returns the state for the composite of active key and given namespace. This is typically used
     * by queryable state.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @return the state of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    public S get(K key, N namespace) {
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
        return get(key, keyGroup, namespace);
    }

    public Stream<K> getKeys(N namespace) {
        return Arrays.stream(keyGroupedStateMaps)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .filter(entry -> entry.getNamespace().equals(namespace))
                .map(StateEntry::getKey);
    }

    public Stream<Tuple2<K, N>> getKeysAndNamespaces() {
        return Arrays.stream(keyGroupedStateMaps)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .map(entry -> Tuple2.of(entry.getKey(), entry.getNamespace()));
    }

    public StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return new StateEntryIterator(recommendedMaxNumberOfReturnedRecords);
    }

    // ------------------------------------------------------------------------

    private S get(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).get(key, namespace);
    }

    private boolean containsKey(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).containsKey(key, namespace);
    }

    private void checkKeyNamespacePreconditions(K key, N namespace) {
        Preconditions.checkNotNull(
                key, "No key set. This method should not be called outside of a keyed context.");
        Preconditions.checkNotNull(namespace, "Provided namespace is null.");
    }

    private void remove(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        getMapForKeyGroup(keyGroupIndex).remove(key, namespace);
    }

    private S removeAndGetOld(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).removeAndGetOld(key, namespace);
    }

    // ------------------------------------------------------------------------
    //  access to maps
    // ------------------------------------------------------------------------

    /** Returns the internal data structure. */
    @VisibleForTesting
    public StateMap<K, N, S>[] getState() {
        return keyGroupedStateMaps;
    }

    public int getKeyGroupOffset() {
        return keyGroupRange.getStartKeyGroup();
    }

    @VisibleForTesting
    public StateMap<K, N, S> getMapForKeyGroup(int keyGroupIndex) {
        final int pos = indexToOffset(keyGroupIndex);
        if (pos >= 0 && pos < keyGroupedStateMaps.length) {
            return keyGroupedStateMaps[pos];
        } else {
            LOG.error("Trying to access key-group {} in pos {}, while the keyGroupedStateMaps.length is {}",
                    keyGroupIndex, pos, keyGroupedStateMaps.length);
            String msg = keyContext.isUsingFlexibleKeyGroupRange() ?
                    keyContext.getFlexibleKeyGroupRange().toString() : keyGroupRange.toString();

            throw new IllegalArgumentException(
                    String.format(
                            "Key group %d is not in %s. Unless you're directly using low level state access APIs, this"
                                    + " is most likely caused by non-deterministic shuffle key (hashCode and equals implementation).",
                            keyGroupIndex, msg));
        }
    }

    /** Translates a key-group id to the internal array offset. */
    public int indexToOffset(int index) {
        if (keyContext.isUsingFlexibleKeyGroupRange()) {
            return keyContext.getFlexibleKeyGroupRange().keyGroupIndexToOffset(index);
        }
        return index - getKeyGroupOffset();
    }

    // Meta data setter / getter and toString -----------------------------------------------------

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<S> getStateSerializer() {
        return metaInfo.getStateSerializer();
    }

    public TypeSerializer<N> getNamespaceSerializer() {
        return metaInfo.getNamespaceSerializer();
    }

    public RegisteredKeyValueStateBackendMetaInfo<N, S> getMetaInfo() {
        return metaInfo;
    }

    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        this.metaInfo = metaInfo;
    }

    // Snapshot / Restore -------------------------------------------------------------------------

    public void put(K key, int keyGroup, N namespace, S state) {
        checkKeyNamespacePreconditions(key, namespace);
        getMapForKeyGroup(keyGroup).put(key, namespace, state);
    }

    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        return Arrays.stream(keyGroupedStateMaps)
                .filter(Objects::nonNull)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .iterator();
    }

    // For testing --------------------------------------------------------------------------------

    @VisibleForTesting
    public int sizeOfNamespace(Object namespace) {
        int count = 0;
        for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
            count += stateMap.sizeOfNamespace(namespace);
        }

        return count;
    }

    @Nonnull
    @Override
    public StateSnapshotKeyGroupReader keyGroupReader(int readVersion) {
        return StateTableByKeyGroupReaders.readerForVersion(this, readVersion);
    }



    // StateEntryIterator
    // ---------------------------------------------------------------------------------------------

    class StateEntryIterator implements StateIncrementalVisitor<K, N, S> {

        final int recommendedMaxNumberOfReturnedRecords;

        int keyGroupIndex;

        StateIncrementalVisitor<K, N, S> stateIncrementalVisitor;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords) {
            this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
            this.keyGroupIndex = 0;
            next();
        }

        private void next() {
            while (keyGroupIndex < keyGroupedStateMaps.length) {
                StateMap<K, N, S> stateMap = keyGroupedStateMaps[keyGroupIndex++];
                StateIncrementalVisitor<K, N, S> visitor =
                        stateMap.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
                if (visitor.hasNext()) {
                    stateIncrementalVisitor = visitor;
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            while (stateIncrementalVisitor == null || !stateIncrementalVisitor.hasNext()) {
                if (keyGroupIndex == keyGroupedStateMaps.length) {
                    return false;
                }
                StateIncrementalVisitor<K, N, S> visitor =
                        keyGroupedStateMaps[keyGroupIndex++].getStateIncrementalVisitor(
                                recommendedMaxNumberOfReturnedRecords);
                if (visitor.hasNext()) {
                    stateIncrementalVisitor = visitor;
                    break;
                }
            }
            return true;
        }

        @Override
        public Collection<StateEntry<K, N, S>> nextEntries() {
            if (!hasNext()) {
                return null;
            }

            return stateIncrementalVisitor.nextEntries();
        }

        @Override
        public void remove(StateEntry<K, N, S> stateEntry) {
            keyGroupedStateMaps[keyGroupIndex - 1].remove(
                    stateEntry.getKey(), stateEntry.getNamespace());
        }

        @Override
        public void update(StateEntry<K, N, S> stateEntry, S newValue) {
            keyGroupedStateMaps[keyGroupIndex - 1].put(
                    stateEntry.getKey(), stateEntry.getNamespace(), newValue);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // scale utilities
    // ---------------------------------------------------------------------------------------------

    public ScalingContext.ContextInitInfo getScalingContextInitInfo(){
        checkState(enableHierarchicalState, "Hierarchical state is not enabled");
        FlexibleKeyGroupRange keyGroupRangeOnceScaled = keyContext.getFlexibleKeyGroupRange();
        Map<Integer,BitSet> allBinFlags= new HashMap<>();
        int idx = 0;
        for (Integer keyGroup : keyGroupRangeOnceScaled) {
            if (idx != keyGroupRangeOnceScaled.keyGroupIndexToOffset(keyGroup)){
                LOG.error("keyGroup {} in {} with offset {}, but current idx is {}", keyGroup, keyGroupRangeOnceScaled, keyGroupRangeOnceScaled.keyGroupIndexToOffset(keyGroup), idx);
                throw new IllegalStateException("keyGroupRangeOnceScaled is not consistent with keyGroupedStateMaps");
            }
            HierarchicalCopyOnWriteStateMap<K, N, S> hierarchicalStateMap =
                    (HierarchicalCopyOnWriteStateMap<K, N, S>) keyGroupedStateMaps[idx++];
            BitSet binFlags = hierarchicalStateMap.getBinFlags();
            LOG.info("init binFlags {} for keyGroup {}", System.identityHashCode(binFlags), keyGroup);
            allBinFlags.put(keyGroup, binFlags);
        }
        return new ScalingContext.ContextInitInfo(
                keyGroupRangeOnceScaled,
                allBinFlags,
                this::clearStateTableForNewlyCreatedTask
        );
    }

    void clearStateTableForNewlyCreatedTask(){
        keyContext.getFlexibleKeyGroupRange().clear();
        for (StateMap stateMap : keyGroupedStateMaps) {
            ((HierarchicalCopyOnWriteStateMap) stateMap).clearAllBins();
        }
    }

    public ScaleBuffer.StateBuffer getStateBuffer(KeyOrStateID keyOrStateID, String stateName) {
        checkState(keyContext.isUsingFlexibleKeyGroupRange(), "keyGroupRange is not flexible");
        final FlexibleKeyGroupRange keyGroupRangeOnceScaled = keyContext.getFlexibleKeyGroupRange();

        checkState(keyGroupRangeOnceScaled.contains(keyOrStateID.getKey()),
                "keyGroupRange " + keyGroupRangeOnceScaled + " does not contain " + keyOrStateID);

        int stateMapIndex = keyGroupRangeOnceScaled.keyGroupIndexToOffset(keyOrStateID.getKey());
        LOG.info("Get state buffer for {} in stateMapIndex {}, keyGroupRange: {}", keyOrStateID, stateMapIndex, keyGroupRangeOnceScaled);

        if (keyOrStateID.isKey()){
            return getStateBufferByKey(keyOrStateID.getKey(), stateMapIndex, stateName, keyGroupRangeOnceScaled);
        }else{
            return getStateBufferByBinID(keyOrStateID.getStateID(), stateMapIndex, stateName);
        }
    }
    public BitSet mergeStateTable(Map<KeyOrStateID, StateMap> managedKeyedStates) {
        checkState(keyContext.isUsingFlexibleKeyGroupRange(), "keyGroupRange is not flexible");
        checkState(enableHierarchicalState, "Hierarchical state is not enabled");
        if (managedKeyedStates.isEmpty()) {
            LOG.warn("empty incoming state table, skip merge");
            return null;
        }
        checkState(managedKeyedStates.size() == 1, "Only one state table is supported");
        KeyOrStateID keyOrStateID = managedKeyedStates.keySet().iterator().next();
        StateMap stateMap = managedKeyedStates.get(keyOrStateID);
        if (keyOrStateID.isKey()) {
            return mergeStateTableByKey(keyOrStateID.getKey(), stateMap);
        } else {
            return mergeStateTableByStateID(keyOrStateID.getStateID(), stateMap);
        }
    }

    public Tuple3<TypeSerializer,TypeSerializer,TypeSerializer> getSerializers(){
        return Tuple3.of(
                keySerializer.duplicate(),
                metaInfo.getNamespaceSerializer().duplicate(),
                metaInfo.getStateSerializer().duplicate());
    }

    public StateSnapshotTransformer<S> getStateSnapshotTransformer(){
        return getMetaInfo()
                .getStateSnapshotTransformFactory()
                .createForDeserializedState()
                .orElse(null);
    }


    ScaleBuffer.StateBuffer getStateBufferByKey(
            int key, int index, String stateName, FlexibleKeyGroupRange keyGroupRangeOnceScaled) {
        checkState(enableHierarchicalState, "Hierarchical state is not enabled");

        HierarchicalCopyOnWriteStateMap<K, N, S> hierarchicalStateMap =
                (HierarchicalCopyOnWriteStateMap<K, N, S>) keyGroupedStateMaps[index];
        StateMap<K, N, S>[] restStateMaps = new StateMap[keyGroupedStateMaps.length - 1];
        int restIndex = 0;
        for (int i = 0; i < keyGroupedStateMaps.length; i++) {
            if (i != index) {
                restStateMaps[restIndex++] = keyGroupedStateMaps[i];
            }
        }
        this.keyGroupedStateMaps = restStateMaps;
        keyGroupRangeOnceScaled.remove(key);

        if (hierarchicalStateMap.isEmpty()) {
            return ScaleBuffer.StateBuffer.EMPTY_STATE_BUFFER;
        }
        ScaleBuffer.StateBuffer buffer = new ScaleBuffer.StateBuffer(
                stateName, Map.of(new KeyOrStateID(key), hierarchicalStateMap));
        buffer.binFlags = hierarchicalStateMap.getBinFlags();
        LOG.info("Move state buffer with binFlags {}({}) for key {}", hierarchicalStateMap.getBinFlags(),
                System.identityHashCode(hierarchicalStateMap.getBinFlags())
                , key);
        return buffer;
    }

    ScaleBuffer.StateBuffer getStateBufferByBinID(HierarchicalStateID stateID, int index, String stateName) {
        checkState(enableHierarchicalState, "Hierarchical state is not enabled");

        HierarchicalCopyOnWriteStateMap<K, N, S> hierarchicalStateMap =
                (HierarchicalCopyOnWriteStateMap<K, N, S>) keyGroupedStateMaps[index];

        Map<KeyOrStateID,StateMap<K, N, S>> outgoingStateMaps =
                Map.of(new KeyOrStateID(stateID), hierarchicalStateMap.getBin(stateID.binIndex));


        ScaleBuffer.StateBuffer res = new ScaleBuffer.StateBuffer(stateName, outgoingStateMaps);

        BitSet binFlags = hierarchicalStateMap.getBinFlags();
        LOG.info("Collected state bin: {} with rest bin{}({}}" , res, binFlags, System.identityHashCode(binFlags));

        if (hierarchicalStateMap.isEmpty()) {

            StateMap<K, N, S>[] restStateMaps = new StateMap[keyGroupedStateMaps.length - 1];
            int restIndex = 0;
            for (int i = 0; i < keyGroupedStateMaps.length; i++) {
                if (i != index) {
                    restStateMaps[restIndex++] = keyGroupedStateMaps[i];
                }
            }
            this.keyGroupedStateMaps = restStateMaps;
            keyContext.getFlexibleKeyGroupRange().remove(stateID.keyGroupIndex);
            LOG.info("Remove empty hierarchicalStateMap {} due to last bin removed, left keyGroupRange: {}",
                    stateID.keyGroupIndex, keyContext.getFlexibleKeyGroupRange());
        }

        checkState(!hierarchicalStateMap.binFlags.get(stateID.binIndex), "binFlag for" + stateID + " is not cleared");

        return res;
    }

    BitSet mergeStateTableByStateID(HierarchicalStateID stateID, StateMap<K, N, S> stateMap) {
        checkState(enableHierarchicalState, "Hierarchical state is not enabled");

        final FlexibleKeyGroupRange keyGroupRangeOnceScaled = keyContext.getFlexibleKeyGroupRange();

        if (keyGroupRangeOnceScaled.contains(stateID.keyGroupIndex)){
            int arrIndex = keyGroupRangeOnceScaled.keyGroupIndexToOffset(stateID.keyGroupIndex);
            HierarchicalCopyOnWriteStateMap<K, N, S> hierarchicalStateMap =
                    (HierarchicalCopyOnWriteStateMap<K, N, S>) keyGroupedStateMaps[arrIndex];
            hierarchicalStateMap.putBin(stateID.binIndex, stateMap);
            return hierarchicalStateMap.getBinFlags();
        }else{
            // first bin of this keyGroup
            HierarchicalCopyOnWriteStateMap<K, N, S> hierarchicalStateMap =
                    HierarchicalCopyOnWriteStateMap.createEmptyMap(getStateSerializer());
            hierarchicalStateMap.putBin(stateID.binIndex, stateMap);
            mergeStateTableByKey(stateID.keyGroupIndex, hierarchicalStateMap);
            return hierarchicalStateMap.getBinFlags();
        }
    }

    BitSet mergeStateTableByKey(int key, StateMap<K, N, S> stateMap) {
        final FlexibleKeyGroupRange keyGroupRangeOnceScaled = keyContext.getFlexibleKeyGroupRange();
        if (keyGroupRangeOnceScaled.contains(key)){
            // there is at least one bin already, just merge two stateMaps
            int arrIndex = keyGroupRangeOnceScaled.keyGroupIndexToOffset(key);
            LOG.info("Get {} -> {} in {}", key, arrIndex, keyGroupRangeOnceScaled);
            moveBinsFromFirstToSecond(
                    (HierarchicalCopyOnWriteStateMap) stateMap,
                    (HierarchicalCopyOnWriteStateMap) keyGroupedStateMaps[arrIndex]);
            stateMap = null; // for on-heap gc
            BitSet currentBinFlags = ((HierarchicalCopyOnWriteStateMap<K, N, S>) keyGroupedStateMaps[arrIndex]).getBinFlags();
            return currentBinFlags;
        }else{
            LOG.info("create new state map for {} with current keygroups {}", key, keyGroupRangeOnceScaled);
            Map<Integer, Integer> originKeyGroupIndex2ArrIndex = keyGroupRangeOnceScaled.keyGroupIndexToOffsetMap();

            keyGroupRangeOnceScaled.add(key);
            StateMap<K, N, S>[] newKeyGroupedStateMaps = new StateMap[originKeyGroupIndex2ArrIndex.size() + 1];
            int currentArrIdx = 0;
            for (Integer keyGroup : keyGroupRangeOnceScaled) {
                if (keyGroup == key){
                    //LOG.info("key {}: new index {}", key, currentArrIdx);
                    newKeyGroupedStateMaps[currentArrIdx++] = stateMap;
                }else{
                    //LOG.info("key {}: origin index {} -> new index {}", keyGroup, originKeyGroupIndex2ArrIndex.get(keyGroup), currentArrIdx);
                    newKeyGroupedStateMaps[currentArrIdx++] = keyGroupedStateMaps[originKeyGroupIndex2ArrIndex.get(keyGroup)];
                }
            }
            this.keyGroupedStateMaps = newKeyGroupedStateMaps;
            BitSet currentBinFlags = ((HierarchicalCopyOnWriteStateMap<K, N, S>) stateMap).getBinFlags();
            LOG.info("Return binFlags {}({}) for key {}", currentBinFlags, System.identityHashCode(currentBinFlags), key);
            return currentBinFlags;
        }
    }

    void moveBinsFromFirstToSecond(HierarchicalCopyOnWriteStateMap<K, N, S> from, HierarchicalCopyOnWriteStateMap<K, N, S> to) {
        BitSet fromBinFlags = from.getBinFlags();
        LOG.info("Move bins {} to local {}", fromBinFlags.cardinality(), to.getBinFlags().cardinality());
        for (int i = 0; i < ScaleConfig.Instance.HIERARCHICAL_BIN_NUM; i++) {
            if (fromBinFlags.get(i)) {
                to.putBin(i, from.getBin(i));
            }
        }
    }

    public abstract StateMap<K, N, S> createStateMap( @Nullable BitSet initialBinFlags);

    public boolean isKeyValid(int keyGroupIndex, int binIndex) {
        if (!enableHierarchicalState){
            return true;
        }
        KeyGroupRange keyGroupRange = keyContext.getKeyGroupRange();
        if (keyGroupRange.contains(keyGroupIndex)){
            int offset = keyGroupRange.keyGroupIndexToOffset(keyGroupIndex);
            HierarchicalCopyOnWriteStateMap stateMap = (HierarchicalCopyOnWriteStateMap) keyGroupedStateMaps[offset];
            return stateMap.isBinSet(binIndex);
        }else{
            LOG.warn("try set current key group index {}, which is not in key group range {}", keyGroupIndex, keyGroupRange);
            return false;
        }
    }
}
