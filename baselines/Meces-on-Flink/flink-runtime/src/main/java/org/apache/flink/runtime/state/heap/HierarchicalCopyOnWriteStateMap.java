package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.MathUtils;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is used to implement Hierarchical State Data Organization
 */
public class HierarchicalCopyOnWriteStateMap<K, N, S> extends CopyOnWriteStateMap<K, N, S>  {
    final static int numOfBins = ScaleConfig.Instance.HIERARCHICAL_BIN_NUM;

    final CopyOnWriteStateMap<K, N, S>[] stateMaps;
    final BitSet binFlags;

    public HierarchicalCopyOnWriteStateMap(TypeSerializer<S> stateSerializer) {
        super();
        stateMaps = new CopyOnWriteStateMap[numOfBins];
        binFlags = new BitSet(numOfBins);
        binFlags.set(0, numOfBins);
        for (int i = 0; i < numOfBins; i++) {
            stateMaps[i] = new CopyOnWriteStateMap<>(stateSerializer);
        }
    }

    static public HierarchicalCopyOnWriteStateMap createEmptyMap(TypeSerializer stateSerializer){
        return new HierarchicalCopyOnWriteStateMap(stateSerializer, new BitSet(numOfBins));
    }

    public HierarchicalCopyOnWriteStateMap(TypeSerializer<S> stateSerializer, BitSet initBinFlags){
        super();
        stateMaps = new CopyOnWriteStateMap[numOfBins];
        this.binFlags = initBinFlags;
        for (int i = 0; i < numOfBins; i++) {
            if (binFlags.get(i)){
                stateMaps[i] = new CopyOnWriteStateMap<>(stateSerializer);
            }
        }
    }

    int getFirstSetBit(){
        return binFlags.nextSetBit(0);
    }

    public static int reHash(Object key){
        return MathUtils.murmurHash(key.hashCode()) % numOfBins;
    }

    public BitSet getBinFlags() {
        return binFlags;
    }
    public boolean isBinSet(int binIndex){
        return binFlags.get(binIndex);
    }

    public StateMap getBin(int binIndex){
        if (binIndex < 0 || binIndex >= numOfBins || !binFlags.get(binIndex)){
            LOG.error("Current bin flags: {}({})", binFlags, System.identityHashCode(binFlags));
            throw new IllegalArgumentException("Bin " + binIndex + " is not in local state map");
        }
        LOG.info("Get bin {} from {}", binIndex, System.identityHashCode(binFlags));
        // remove the bin flag
        binFlags.clear(binIndex);
        StateMap stateMap = stateMaps[binIndex];
        stateMaps[binIndex] = null;
        return stateMap;
    }

    public void putBin(int binIndex, StateMap stateMap){
        if (binIndex < 0 || binIndex >= numOfBins || binFlags.get(binIndex)){
            LOG.error("Current bin flags: {}", binFlags);
            throw new IllegalArgumentException("Bin " + binIndex + " is not in local state map or already set");
        }
        // set the bin flag
        LOG.info("Put bin {} to {}", binIndex, System.identityHashCode(binFlags));
        binFlags.set(binIndex);
        stateMaps[binIndex] = (CopyOnWriteStateMap<K, N, S>) stateMap;
    }

    public void clearAllBins() {
        // assert all binFlags are set
        checkState(binFlags.cardinality() == numOfBins, "Not all bins are set during clearAllBins");
        for (int i = 0; i < numOfBins; i++) {
            stateMaps[i] = null;
        }
        binFlags.clear();
    }

    public boolean isEmpty(){
        return binFlags.isEmpty();
    }

    @Override
    public int size(){
        int size = 0;
        for (int i = 0; i < numOfBins; i++) {
            if (!binFlags.get(i)){
                continue;
            }
            size += stateMaps[i].size();
        }
        return size;
    }
    @Override
    public S get(K key, N namespace) {
        checkNotNull(stateMaps[reHash(key)], "Bin : %s is null", reHash(key));
        return stateMaps[reHash(key)].get(key, namespace);
    }
    @Override
    public boolean containsKey(K key, N namespace) {
        return stateMaps[reHash(key)].containsKey(key, namespace);
    }
    @Override
    public void put(K key, N namespace, S value) {
        stateMaps[reHash(key)].put(key, namespace, value);
    }
    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        return stateMaps[reHash(key)].putAndGetOld(key, namespace, state);
    }
    @Override
    public void remove(K key, N namespace) {
        stateMaps[reHash(key)].remove(key, namespace);
    }
    @Override
    public S removeAndGetOld(K key, N namespace) {
        return stateMaps[reHash(key)].removeAndGetOld(key, namespace);
    }
    @Override
    public Stream<K> getKeys(N namespace) {
        return IntStream.range(0, numOfBins)
                .filter(binFlags::get)
                .mapToObj(i -> stateMaps[i].getKeys(namespace))
                .flatMap(stream -> stream);
    }
    @Override
    public <T> void transform(
            K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        stateMaps[reHash(key)].transform(key, namespace, value, transformation);
    }

    @Nonnull
    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        checkState(!isEmpty(), "The state map is empty.");
        return new HierarchicalStateEntryIterator();
    }

    @VisibleForTesting
    @Override
    void releaseSnapshot(int snapshotVersion) {
        for (int i = 0; i < numOfBins; i++) {
            if (!binFlags.get(i)){
                continue;
            }
            stateMaps[i].releaseSnapshot(snapshotVersion);
        }
    }

    @VisibleForTesting
    @Override
    StateMapEntry<K, N, S>[] snapshotMapArrays() {
        return IntStream.range(0, numOfBins)
                .filter(binFlags::get)
                .mapToObj(i -> Arrays.stream(stateMaps[i].snapshotMapArrays()))
                .flatMap(stream -> stream)
                .toArray(StateMapEntry[]::new);
    }

    @Override
    int getStateMapVersion() {
        return stateMaps[getFirstSetBit()].getStateMapVersion();
    }

    @Override
    @VisibleForTesting
    boolean isRehashing() {
        return IntStream.range(0, numOfBins)
                .filter(binFlags::get)
                .anyMatch(i -> stateMaps[i].isRehashing());
    }

    @Override
    @VisibleForTesting
    Set<Integer> getSnapshotVersions() {
        return stateMaps[getFirstSetBit()].getSnapshotVersions();
    }

    @Override
    public TypeSerializer<S> getStateSerializer() {
        return stateMaps[getFirstSetBit()].stateSerializer;
    }

    @Override
    public void setStateSerializer(TypeSerializer<S> stateSerializer) {
        for (int i = 0; i < numOfBins; i++) {
            if (!binFlags.get(i)){
                continue;
            }
            stateMaps[i].setStateSerializer(stateSerializer);
        }
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        int count = 0;
        for (int i = 0; i < numOfBins; i++) {
            if (!binFlags.get(i)){
                continue;
            }
            count += stateMaps[i].sizeOfNamespace(namespace);
        }
        return count;
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        checkState(!isEmpty(), "The state map is empty.");
        return new HierarchicalStateIncrementalVisitorImpl(recommendedMaxNumberOfReturnedRecords);
    }


    class HierarchicalStateEntryIterator extends StateEntryIterator {
        private int currentBinIndex;
        private Iterator<StateEntry<K, N, S>> currentIterator;

        HierarchicalStateEntryIterator() {
            currentBinIndex = 0;
            while (currentBinIndex < numOfBins && !binFlags.get(currentBinIndex)) {
                currentBinIndex++;
            }
            currentIterator = stateMaps[currentBinIndex].iterator();
        }

        @Override
        public boolean hasNext() {
            while (!currentIterator.hasNext() && currentBinIndex < numOfBins - 1) {
                for (currentBinIndex++; currentBinIndex < numOfBins && !binFlags.get(currentBinIndex); currentBinIndex++);

                if (currentBinIndex < numOfBins) {
                    currentIterator = stateMaps[currentBinIndex].iterator();
                }
            }
            return currentIterator.hasNext();
        }

        @Override
        public StateEntry<K, N, S> next() {
            return currentIterator.next();
        }
    }

    class HierarchicalStateIncrementalVisitorImpl extends StateIncrementalVisitorImpl{
        private int currentBinIndex;
        private InternalKvState.StateIncrementalVisitor chainIterator;
        final int recommendedMaxNumberOfReturnedRecords;

        HierarchicalStateIncrementalVisitorImpl(int recommendedMaxNumberOfReturnedRecords) {
            super();
            this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
            currentBinIndex = 0;
            while (currentBinIndex < numOfBins && !binFlags.get(currentBinIndex)) {
                currentBinIndex++;
            }
            chainIterator = stateMaps[currentBinIndex].getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
        }

        @Override
        public boolean hasNext() {
            while (!chainIterator.hasNext() && currentBinIndex < numOfBins - 1) {
                for (currentBinIndex++; currentBinIndex < numOfBins && !binFlags.get(currentBinIndex); currentBinIndex++);
                if (currentBinIndex < numOfBins) {
                    chainIterator = stateMaps[currentBinIndex].getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
                }
            }
            return chainIterator.hasNext();
        }

        @Override
        public Collection<StateEntry<K, N, S>> nextEntries(){
            Collection<StateEntry<K, N, S>> entries = chainIterator.nextEntries();
            while (entries == null && currentBinIndex < numOfBins - 1) {
                for (currentBinIndex++; currentBinIndex < numOfBins && !binFlags.get(currentBinIndex); currentBinIndex++);
                if (currentBinIndex < numOfBins) {
                    chainIterator = stateMaps[currentBinIndex].getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
                    entries = chainIterator.nextEntries();
                }
            }
            return entries;
        }
        @Override
        public void remove(StateEntry<K, N, S> stateEntry) {
            HierarchicalCopyOnWriteStateMap.this.remove(stateEntry.getKey(), stateEntry.getNamespace());
        }

        @Override
        public void update(StateEntry<K, N, S> stateEntry, S newValue) {
            HierarchicalCopyOnWriteStateMap.this.put(stateEntry.getKey(), stateEntry.getNamespace(), newValue);
        }

    }
}
