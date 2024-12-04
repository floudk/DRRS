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

import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A heap-based priority queue with set semantics, based on {@link HeapPriorityQueue}. The heap is
 * supported by hash set for fast contains (de-duplication) and deletes. Object identification
 * happens based on {@link #equals(Object)}.
 *
 * <p>Possible future improvements:
 *
 * <ul>
 *   <li>We could also implement shrinking for the heap and the deduplication set.
 *   <li>We could replace the deduplication maps with more efficient custom implementations. In
 *       particular, a hash set would be enough if it could return existing elements on unsuccessful
 *       adding, etc..
 * </ul>
 *
 * @param <T> type of the contained elements.
 */
public class HeapPriorityQueueSet<T extends HeapPriorityQueueElement> extends HeapPriorityQueue<T>
        implements KeyGroupedInternalPriorityQueue<T> {

    /** Function to extract the key from contained elements. */
    private final KeyExtractorFunction<T> keyExtractor;

    /**
     * This array contains one hash set per key-group. The sets are used for fast de-duplication and
     * deletes of elements.
     */
    private final List<HashMap<T, T>> deduplicationMapsByKeyGroup;

    /** The key-group range of elements that are managed by this queue. */
    private KeyGroupRange keyGroupRange;

    /** The total number of key-groups of the job. */
    private final int totalNumberOfKeyGroups;

    private final int deduplicationSetSize;

    /**
     * Creates an empty {@link HeapPriorityQueueSet} with the requested initial capacity.
     *
     * @param elementPriorityComparator comparator for the priority of contained elements.
     * @param keyExtractor function to extract a key from the contained elements.
     * @param minimumCapacity the minimum and initial capacity of this priority queue.
     * @param keyGroupRange the key-group range of the elements in this set.
     * @param totalNumberOfKeyGroups the total number of key-groups of the job.
     */
    @SuppressWarnings("unchecked")
    public HeapPriorityQueueSet(
            @Nonnull PriorityComparator<T> elementPriorityComparator,
            @Nonnull KeyExtractorFunction<T> keyExtractor,
            @Nonnegative int minimumCapacity,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int totalNumberOfKeyGroups) {

        super(elementPriorityComparator, minimumCapacity);

        this.keyExtractor = keyExtractor;

        this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;

        final int keyGroupsInLocalRange = keyGroupRange.getNumberOfKeyGroups();
        this.deduplicationSetSize = 1 + minimumCapacity / keyGroupsInLocalRange;
        this.deduplicationMapsByKeyGroup = new ArrayList<>(keyGroupsInLocalRange);
        for (int i = 0; i < keyGroupsInLocalRange; ++i) {
            deduplicationMapsByKeyGroup.add(new HashMap<>(deduplicationSetSize));
        }
    }

    @Override
    @Nullable
    public T poll() {
        final T toRemove = super.poll();
        return toRemove != null ? getDedupMapForElement(toRemove).remove(toRemove) : null;
    }

    /**
     * Adds the element to the queue. In contrast to the superclass and to maintain set semantics,
     * this happens only if no such element is already contained (determined by {@link
     * #equals(Object)}).
     *
     * @return <code>true</code> if the operation changed the head element or if is it unclear if
     *     the head element changed. Only returns <code>false</code> iff the head element was not
     *     changed by this operation.
     */
    @Override
    public boolean add(@Nonnull T element) {
        return getDedupMapForElement(element).putIfAbsent(element, element) == null
                && super.add(element);
    }

    /**
     * In contrast to the superclass and to maintain set semantics, removal here is based on
     * comparing the given element via {@link #equals(Object)}.
     *
     * @return <code>true</code> if the operation changed the head element or if is it unclear if
     *     the head element changed. Only returns <code>false</code> iff the head element was not
     *     changed by this operation.
     */
    @Override
    public boolean remove(@Nonnull T toRemove) {
        T storedElement = getDedupMapForElement(toRemove).remove(toRemove);
        return storedElement != null && super.remove(storedElement);
    }

    @Override
    public void clear() {
        super.clear();
        for (HashMap<?, ?> elementHashMap : deduplicationMapsByKeyGroup) {
            elementHashMap.clear();
        }
    }

    private HashMap<T, T> getDedupMapForKeyGroup(@Nonnegative int keyGroupId) {
        return deduplicationMapsByKeyGroup.get(globalKeyGroupToLocalIndex(keyGroupId));
    }


    private HashMap<T, T> getDedupMapForElement(T element) {
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyExtractor.extractKeyFromElement(element), totalNumberOfKeyGroups);
        return getDedupMapForKeyGroup(keyGroup);
    }

    private int globalKeyGroupToLocalIndex(int keyGroup) {
        checkArgument(
                keyGroupRange.contains(keyGroup),
                "%s does not contain key group %s",
                keyGroupRange,
                keyGroup);
//        return keyGroup - keyGroupRange.getStartKeyGroup();
        int offset = keyGroupRange.keyGroupIndexToOffset(keyGroup);
        if (offset >= deduplicationMapsByKeyGroup.size()) {
            LOG.warn(
                    "Key group {} (index {}) is not in the expected range {}(length: {})",
                    keyGroup,
                    offset,
                    keyGroupRange,
                    deduplicationMapsByKeyGroup.size());
        }
        return offset;
    }

    @Nonnull
    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        return getDedupMapForKeyGroup(keyGroupId).keySet();
    }

    // ------------------------------------------------------------------------
    // Scale Utils
    // ------------------------------------------------------------------------



    @Nonnull
    @Override
    public Set<T> getAndRemoveSubsetForKeyGroup(int keyGroupId) {
        HashMap<T, T> dedupMap = getDedupMapForKeyGroup(keyGroupId);
        Set<T> subset = dedupMap.keySet();
        for (T element : subset) {
            super.remove(element);
        }
        dedupMap.clear();
        // remove keyGroupID from deduplicationMapsByKeyGroup
        deduplicationMapsByKeyGroup.remove(globalKeyGroupToLocalIndex(keyGroupId));
        return subset;
    }

    @Override
    public void updateKeyGroupRange(KeyGroupRange keyGroupRange) {
        this.keyGroupRange = keyGroupRange;
    }

    @Override
    public void mergeKeyGroup(int mergedKeyGroup){
        if (keyGroupRange.getNumberOfKeyGroups() <= deduplicationMapsByKeyGroup.size()){
            return;
        }
        int offset = keyGroupRange.keyGroupIndexToOffset(mergedKeyGroup);
        // update deduplicationMapsByKeyGroup: adjust the size and order
        deduplicationMapsByKeyGroup.add(offset, new HashMap<>(deduplicationSetSize));
    }
}
