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
import org.apache.flink.runtime.scale.io.message.local.StateBuffer;
import org.apache.flink.runtime.scale.state.FlexibleKeyGroupRange;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.metaInfo = Preconditions.checkNotNull(metaInfo);
        this.keySerializer = Preconditions.checkNotNull(keySerializer);

        this.keyGroupRange = keyContext.getKeyGroupRange();

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
    public void getStateBuffer(
            FlexibleKeyGroupRange outgoingKeyGroupRange,
            StateBuffer migratingState) {

        checkState(keyContext.isUsingFlexibleKeyGroupRange(), "keyGroupRange is not flexible");

        Map<Integer,StateMap<K, N, S>> outgoingStateMaps;
        final FlexibleKeyGroupRange keyGroupRangeOnceScaled = keyContext.getFlexibleKeyGroupRange();


        Map<Integer,Integer> arrIdx2KeyGroupIdx = keyGroupRangeOnceScaled.getOffsetToKeyGroupIndexWithSubRange(outgoingKeyGroupRange);
        // LOG.info("Get arrIdx2KeyGroupIdx: {} from keyGroupRange: {}", arrIdx2KeyGroupIdx, keyGroupRangeOnceScaled);

        outgoingStateMaps =  new HashMap<>(arrIdx2KeyGroupIdx.size());

        StateMap<K, N, S>[] restStateMaps =
                new StateMap[keyGroupedStateMaps.length-arrIdx2KeyGroupIdx.size()];

        int newIndex = 0;
        for(int i = 0; i < keyGroupedStateMaps.length; i++){
            if(arrIdx2KeyGroupIdx.containsKey(i)){
                outgoingStateMaps.put(arrIdx2KeyGroupIdx.get(i), keyGroupedStateMaps[i]);
            }else{
                restStateMaps[newIndex++] = keyGroupedStateMaps[i];
            }
        }

        this.keyGroupedStateMaps = restStateMaps;
        keyGroupRangeOnceScaled.remove(outgoingKeyGroupRange);

        migratingState.outgoingManagedKeyedState = outgoingStateMaps;

        LOG.info("Get state buffer : {} and left keyGroupRange: {}", migratingState, keyGroupRangeOnceScaled);
    }

    // this is not thread safe,
    // but since it only reads the state, it should be fine
    public Map<Integer, Long> getStateSizes(){
        final int collectingThreads = 2;

        // Split key groups into subsets
        List<List<Integer>> keyGroupSubsets = new ArrayList<>(collectingThreads);
        for (int i = 0; i < collectingThreads; i++) {
            keyGroupSubsets.add(new ArrayList<>());
        }

        int i = 0;
        for (int keyGroup : keyGroupRange) {
            keyGroupSubsets.get(i % collectingThreads).add(keyGroup);
            i++;
        }

        // Create futures for each subset
        List<CompletableFuture<Map<Integer, Long>>> futures = new ArrayList<>();
        for (List<Integer> subset : keyGroupSubsets) {
            CompletableFuture<Map<Integer, Long>> future = CompletableFuture.supplyAsync(() -> {
                Map<Integer, Long> subsetSizes = new HashMap<>();
                for (int keyGroup : subset) {
                    subsetSizes.put(keyGroup, getMapForKeyGroup(keyGroup).getStateSizes());
                }
                return subsetSizes;
            });
            futures.add(future);
        }

        // Wait for all futures and combine results
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .flatMap(map -> map.entrySet().stream())
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue)))
                .join();
    }


    public void mergeStateTable(Map<Integer, StateMap> managedKeyedStates) {
        checkState(keyContext.isUsingFlexibleKeyGroupRange(), "keyGroupRange is not flexible");
        final FlexibleKeyGroupRange keyGroupRangeOnceScaled = keyContext.getFlexibleKeyGroupRange();

        if (managedKeyedStates.isEmpty()) {
            LOG.warn("empty incoming state table, skip merge");
            return;
        }

        Map<Integer,Integer> originKeyGroupIndex2ArrIndex =
                keyGroupRangeOnceScaled.keyGroupIndexToOffsetMap();

        keyGroupRangeOnceScaled.add(managedKeyedStates.keySet());


        StateMap<K, N, S>[] newKeyGroupedStateMaps =
                new StateMap[keyGroupRangeOnceScaled.getNumberOfKeyGroups()];

        int currentArrIdx = 0;
        for (Integer keyGroup : keyGroupRangeOnceScaled) {
            if (managedKeyedStates.containsKey(keyGroup)) {
                // new state map
                // for newly-created task, this prior branch will overwrite the initial stateMap
                newKeyGroupedStateMaps[currentArrIdx++] = managedKeyedStates.get(keyGroup);
            } else {
                // origin state map
                newKeyGroupedStateMaps[currentArrIdx++] =
                        keyGroupedStateMaps[originKeyGroupIndex2ArrIndex.get(keyGroup)];
            }
        }

        this.keyGroupedStateMaps = newKeyGroupedStateMaps;
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
}
