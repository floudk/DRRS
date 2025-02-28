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

package org.apache.flink.runtime.scale.state;

import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

public class FlexibleKeyGroupRange extends KeyGroupRange {

    private final TreeMap<Integer,KeyGroupRange> keyGroupRanges;

    FlexibleKeyGroupRange(TreeMap<Integer, KeyGroupRange> keyGroupRanges) {
        this.keyGroupRanges = keyGroupRanges;
    }

    public static KeyGroupRange fromKeyGroupRangeList(List<KeyGroupRange> maybeUnmodifiableKeyGroupRanges) {
        if(maybeUnmodifiableKeyGroupRanges.isEmpty()) {
            return EMPTY_KEY_GROUP_RANGE;
        }
        TreeMap<Integer, KeyGroupRange> mergedRanges = new TreeMap<>();
        List<KeyGroupRange> sortedKeyGroupRangeList = new ArrayList<>(maybeUnmodifiableKeyGroupRanges);

        sortedKeyGroupRangeList.sort((a, b) -> {
            if (a.getStartKeyGroup() != b.getStartKeyGroup()) {
                return Integer.compare(a.getStartKeyGroup(), b.getStartKeyGroup());
            }
            return Integer.compare(a.getEndKeyGroup(), b.getEndKeyGroup());
        });

        KeyGroupRange currentKeyGroupRange = sortedKeyGroupRangeList.get(0);
        for (int i = 1; i < sortedKeyGroupRangeList.size(); i++) {
            KeyGroupRange nextKeyGroupRange = sortedKeyGroupRangeList.get(i);
            if (currentKeyGroupRange.getEndKeyGroup() + 1 >= nextKeyGroupRange.getStartKeyGroup()) {

                currentKeyGroupRange = new KeyGroupRange(currentKeyGroupRange.getStartKeyGroup(),
                        Math.max(currentKeyGroupRange.getEndKeyGroup(), nextKeyGroupRange.getEndKeyGroup()));
            } else {

                mergedRanges.put(currentKeyGroupRange.getStartKeyGroup(), currentKeyGroupRange);
                currentKeyGroupRange = nextKeyGroupRange;
            }
        }
        mergedRanges.put(currentKeyGroupRange.getStartKeyGroup(), currentKeyGroupRange);
        return new FlexibleKeyGroupRange(mergedRanges);
    }

    public static KeyGroupRange fromKeyGroupRange(KeyGroupRange keyGroupRange) {
        checkArgument(!(keyGroupRange instanceof FlexibleKeyGroupRange),
                "The input key group range should not be an instance of FlexibleKeyGroupRange");
        if (keyGroupRange == null || keyGroupRange.equals(EMPTY_KEY_GROUP_RANGE)) {
            return EMPTY_KEY_GROUP_RANGE;
        } else {
            return fromKeyGroupRangeList(Collections.singletonList(keyGroupRange));
        }
    }
    public static KeyGroupRange fromIntegerList(List<Integer> keyGroups) {
        if(keyGroups.isEmpty()) {
            return EMPTY_KEY_GROUP_RANGE;
        }
        // sort the key groups
        try{
            keyGroups.sort(Integer::compareTo);
        }catch (UnsupportedOperationException e){
            // if the input list is unmodifiable, create a new list
            keyGroups = new ArrayList<>(keyGroups);
            keyGroups.sort(Integer::compareTo);
        }
        TreeMap<Integer, KeyGroupRange> keyGroupRanges = new TreeMap<>();
        int start = keyGroups.get(0);
        int end = start;
        for (int i = 1; i < keyGroups.size(); i++) {
            if (keyGroups.get(i) == end + 1) {
                end++;
            } else {
                keyGroupRanges.put(start, new KeyGroupRange(start, end));
                start = keyGroups.get(i);
                end = start;
            }
        }
        keyGroupRanges.put(start, new KeyGroupRange(start, end));
        return new FlexibleKeyGroupRange(keyGroupRanges);
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        } else if(o instanceof FlexibleKeyGroupRange) {
            FlexibleKeyGroupRange other = (FlexibleKeyGroupRange) o;
            return keyGroupRanges.equals(other.keyGroupRanges);
        } else if (o instanceof KeyGroupRange){
            KeyGroupRange other = (KeyGroupRange) o;
            return keyGroupRanges.size() == 1 && keyGroupRanges.firstEntry().getValue().equals(other);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return keyGroupRanges.hashCode();
    }

    @Override
    public String toString() {
        if (keyGroupRanges.isEmpty() || this.equals(EMPTY_KEY_GROUP_RANGE)) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        keyGroupRanges.values().forEach(
                range -> sb.append(
                        range.getStartKeyGroup())
                        .append("-")
                        .append(range.getEndKeyGroup())
                        .append(","));

        return "[" + sb.substring(0, sb.length() - 1) + "]";
    }

    public void clear() {
        this.keyGroupRanges.clear();
    }

    // -------------------------------------- utils ----------------------------------------

    @Override
    public boolean contains(int keyGroup) {
        Integer floorKey = keyGroupRanges.floorKey(keyGroup);
        return floorKey != null && keyGroupRanges.get(floorKey).contains(keyGroup);
    }

    @Override
    public KeyGroupRange getIntersection(KeyGroupRange other) {
        TreeMap<Integer, KeyGroupRange> intersectionMap = new TreeMap<>();
        if (other instanceof FlexibleKeyGroupRange){
            FlexibleKeyGroupRange otherFlexible = (FlexibleKeyGroupRange) other;
            for (KeyGroupRange range : keyGroupRanges.values()) {
                for (KeyGroupRange otherRange : otherFlexible.keyGroupRanges.values()) {
                    KeyGroupRange intersection = range.getIntersection(otherRange);
                    if (!intersection.equals(EMPTY_KEY_GROUP_RANGE)) {
                        intersectionMap.put(intersection.getStartKeyGroup(), intersection);
                    }
                }
            }

        }else{
            for (KeyGroupRange range : keyGroupRanges.values()) {
                KeyGroupRange intersection = range.getIntersection(other);
                if (!intersection.equals(EMPTY_KEY_GROUP_RANGE)) {
                    intersectionMap.put(intersection.getStartKeyGroup(), intersection);
                }
            }
        }
        if (intersectionMap.isEmpty()) {
            return EMPTY_KEY_GROUP_RANGE;
        } else {
            return new FlexibleKeyGroupRange(intersectionMap);
        }
    }

    @Override
    public int getNumberOfKeyGroups() {
        return keyGroupRanges.values().stream().mapToInt(KeyGroupRange::getNumberOfKeyGroups).sum();
    }

    @Override
    public int getStartKeyGroup() {
        return keyGroupRanges.firstKey();
    }

    @Override
    public int getEndKeyGroup() {
        return keyGroupRanges.lastEntry().getValue().getEndKeyGroup();
    }

    @Override
    public int getKeyGroupId(int idx) {
        if (idx < 0 || idx > getNumberOfKeyGroups()) {
            throw new IndexOutOfBoundsException("Key group index out of bounds: " + idx);
        }
        for (KeyGroupRange range : keyGroupRanges.values()) {
            if (idx < range.getNumberOfKeyGroups()) {
                return range.getKeyGroupId(idx);
            }
            idx -= range.getNumberOfKeyGroups();
        }
        throw new IndexOutOfBoundsException("Key group index out of bounds: " + idx);
    }

    @Override
    public Iterator<Integer> iterator() {
        return new FlexibleKeyGroupIterator();
    }

    private class FlexibleKeyGroupIterator implements Iterator<Integer> {
        private final Iterator<KeyGroupRange> keyGroupRangeIterator;
        private Iterator<Integer> currentIterator;

        public FlexibleKeyGroupIterator() {
            keyGroupRangeIterator = keyGroupRanges.values().iterator();
            currentIterator = keyGroupRangeIterator.hasNext() ? keyGroupRangeIterator.next().iterator() : null;
        }

        @Override
        public boolean hasNext() {
            if (currentIterator == null) {
                return false;
            }
            if (currentIterator.hasNext()) {
                return true;
            }
            while (keyGroupRangeIterator.hasNext()) {
                currentIterator = keyGroupRangeIterator.next().iterator();
                if (currentIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Integer next() {
            if (!hasNext()) {
                throw new IllegalStateException("No more key groups to iterate");
            }
            return currentIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Unsupported by this iterator!");
        }
    }


    public boolean containsSubRange(FlexibleKeyGroupRange subRange){
        for (KeyGroupRange otherRange : subRange.keyGroupRanges.values()) {
            Map.Entry<Integer, KeyGroupRange> floorEntry = keyGroupRanges.floorEntry(otherRange.getStartKeyGroup());

            if (floorEntry == null) {
                return false;
            }
            KeyGroupRange currentRange = floorEntry.getValue();

            if (currentRange.getStartKeyGroup() > otherRange.getStartKeyGroup() ||
                    currentRange.getEndKeyGroup() < otherRange.getEndKeyGroup()) {
                return false;
            }
        }
        return true;
    }

    // convert key group index to offset in the whole key group range
    @Override
    public int keyGroupIndexToOffset(int keyGroupIndex) {
        int offset = 0;
        for (KeyGroupRange range : keyGroupRanges.values()) {
            if (range.contains(keyGroupIndex)) {
                return offset + keyGroupIndex - range.getStartKeyGroup();
            }
            offset += range.getNumberOfKeyGroups();
        }
        throw new IllegalArgumentException("Key group index out of bounds: " + keyGroupIndex);
    }

    public Map<Integer,Integer> keyGroupIndexToOffsetMap(){
        Map<Integer,Integer> keyGroupIndex2Offset = new HashMap<>();
        int idx = 0;
        for (KeyGroupRange range : keyGroupRanges.values()) {
            for (int i = range.getStartKeyGroup(); i <= range.getEndKeyGroup(); i++) {
                keyGroupIndex2Offset.put(i, idx);
                idx++;
            }
        }
        return keyGroupIndex2Offset;
    }


    // remove the key group range from current key group range
    public void remove(FlexibleKeyGroupRange tobeRemoved) {
        if (tobeRemoved.equals(this)) {
            this.keyGroupRanges.clear();
            return;
        }
        if (tobeRemoved.equals(EMPTY_KEY_GROUP_RANGE)) {
            return;
        }

        checkState( this.containsSubRange(tobeRemoved),
                "The key group range to be removed is not in the key group range");
        TreeMap<Integer, KeyGroupRange> newKeyGroupRanges = new TreeMap<>();

        Iterator<KeyGroupRange> tobeRemovedIterator = tobeRemoved.keyGroupRanges.values().iterator();
        KeyGroupRange tobeRemovedRange = tobeRemovedIterator.next();
        for (KeyGroupRange currentRange : keyGroupRanges.values()) {
            if (tobeRemovedRange == null) {
                newKeyGroupRanges.put(currentRange.getStartKeyGroup(), currentRange);
            }else if (currentRange.getEndKeyGroup() < tobeRemovedRange.getStartKeyGroup()) {
                newKeyGroupRanges.put(currentRange.getStartKeyGroup(), currentRange);
            } else {
                checkState(currentRange.getStartKeyGroup() <= tobeRemovedRange.getStartKeyGroup() &&
                                currentRange.getEndKeyGroup() >= tobeRemovedRange.getEndKeyGroup(),
                        "The key group range to be removed is not in the key group range");
                if (currentRange.equals(tobeRemovedRange)){
                    // do nothing
                }else if (currentRange.getStartKeyGroup() == tobeRemovedRange.getStartKeyGroup()) {
                    newKeyGroupRanges.put(tobeRemovedRange.getEndKeyGroup() + 1,
                            new KeyGroupRange(tobeRemovedRange.getEndKeyGroup() + 1, currentRange.getEndKeyGroup()));
                } else if (currentRange.getEndKeyGroup() == tobeRemovedRange.getEndKeyGroup()) {
                    newKeyGroupRanges.put(currentRange.getStartKeyGroup(),
                            new KeyGroupRange(currentRange.getStartKeyGroup(), tobeRemovedRange.getStartKeyGroup() - 1));
                } else {
                    newKeyGroupRanges.put(currentRange.getStartKeyGroup(),
                            new KeyGroupRange(currentRange.getStartKeyGroup(), tobeRemovedRange.getStartKeyGroup() - 1));
                    newKeyGroupRanges.put(tobeRemovedRange.getEndKeyGroup() + 1,
                            new KeyGroupRange(tobeRemovedRange.getEndKeyGroup() + 1, currentRange.getEndKeyGroup()));
                }
                tobeRemovedRange = tobeRemovedIterator.hasNext() ? tobeRemovedIterator.next() : null;
            }
        }
        this.keyGroupRanges.clear();
        this.keyGroupRanges.putAll(newKeyGroupRanges);
    }

    private void add(int keyGroupIndex){
        if (keyGroupRanges.isEmpty()) {
            keyGroupRanges.put(keyGroupIndex, new KeyGroupRange(keyGroupIndex, keyGroupIndex));
            return;
        }

        KeyGroupRange floorRange = keyGroupRanges.floorEntry(keyGroupIndex) == null ?
                null : keyGroupRanges.floorEntry(keyGroupIndex).getValue();
        KeyGroupRange higherRange = keyGroupRanges.higherEntry(keyGroupIndex) == null ?
                null : keyGroupRanges.higherEntry(keyGroupIndex).getValue();

       if (floorRange == null) {
             if (keyGroupRanges.firstKey() == keyGroupIndex + 1){
                 KeyGroupRange firstRange = keyGroupRanges.pollFirstEntry().getValue();
                 keyGroupRanges.put(keyGroupIndex, new KeyGroupRange(keyGroupIndex, firstRange.getEndKeyGroup()));
            }else{
                keyGroupRanges.put(keyGroupIndex, new KeyGroupRange(keyGroupIndex, keyGroupIndex));
            }
        } else if(higherRange == null){
            if (floorRange.getEndKeyGroup() == keyGroupIndex - 1) {
                keyGroupRanges.put(
                        floorRange.getStartKeyGroup(),
                        new KeyGroupRange(floorRange.getStartKeyGroup(), keyGroupIndex));
            }else{
                keyGroupRanges.put(keyGroupIndex, new KeyGroupRange(keyGroupIndex, keyGroupIndex));
            }
        } else if (floorRange.getEndKeyGroup() == keyGroupIndex - 1 && higherRange.getStartKeyGroup() == keyGroupIndex + 1){
            keyGroupRanges.remove(higherRange.getStartKeyGroup());
            keyGroupRanges.put(floorRange.getStartKeyGroup(), new KeyGroupRange(floorRange.getStartKeyGroup(), higherRange.getEndKeyGroup()));
        } else if(floorRange.getEndKeyGroup() == keyGroupIndex - 1){
            keyGroupRanges.put(floorRange.getStartKeyGroup(), new KeyGroupRange(floorRange.getStartKeyGroup(), keyGroupIndex));
        } else if (higherRange.getStartKeyGroup() == keyGroupIndex + 1){
            keyGroupRanges.remove(higherRange.getStartKeyGroup());
            keyGroupRanges.put(keyGroupIndex, new KeyGroupRange(keyGroupIndex, higherRange.getEndKeyGroup()));
        } else {
            keyGroupRanges.put(keyGroupIndex, new KeyGroupRange(keyGroupIndex, keyGroupIndex));
        }
    }

    public void add(Set<Integer> integers) {
        if (integers == null || integers.isEmpty()) {
            return;
        }
        for (int keyGroupIndex : integers) {
            add(keyGroupIndex);
        }
    }

    public Map<Integer, Integer> getOffsetToKeyGroupIndexWithSubRange(FlexibleKeyGroupRange outgoingKeyGroupRange) {
        checkState(this.containsSubRange(outgoingKeyGroupRange),
                "The key group range to be removed is not in the key group range");
        Map<Integer, Integer> res = new HashMap<>();

        Iterator <KeyGroupRange> iterator = keyGroupRanges.values().iterator();
        KeyGroupRange currentRange = iterator.next();

        int globalIdx = 0;
        for (Integer outKeyGroup : outgoingKeyGroupRange) {
            while (!currentRange.contains(outKeyGroup)) {
                globalIdx += currentRange.getNumberOfKeyGroups();
                currentRange = iterator.next();
            }
            res.put(globalIdx + outKeyGroup - currentRange.getStartKeyGroup(), outKeyGroup);
        }

        return res;
    }

    public List<Integer> getSymmetricDifference(FlexibleKeyGroupRange other) {
        List<Integer> res = new ArrayList<>();

        // get all key groups:
        // 1. in this key group range but not in other key group range
        // 2. in other key group range but not in this key group range

        KeyGroupRange intersection =  this.getIntersection(other);
        if (intersection.equals(EMPTY_KEY_GROUP_RANGE)) {
            res.addAll(this.toList());
            res.addAll(other.toList());
            return res;
        }
        FlexibleKeyGroupRange temp1 = (FlexibleKeyGroupRange) this.clone();
        temp1.remove((FlexibleKeyGroupRange) intersection);
        FlexibleKeyGroupRange temp2 = (FlexibleKeyGroupRange) other.clone();
        temp2.remove((FlexibleKeyGroupRange) intersection);
        res.addAll(temp1.toList());
        res.addAll(temp2.toList());
        return res;
    }

    public List<Integer> toList() {
        List<Integer> res = new ArrayList<>();
        for (KeyGroupRange range : keyGroupRanges.values()) {
            for (int i = range.getStartKeyGroup(); i <= range.getEndKeyGroup(); i++) {
                res.add(i);
            }
        }
        return res;
    }

    public KeyGroupRange clone() {
        if (this.equals(EMPTY_KEY_GROUP_RANGE)) {
            return EMPTY_KEY_GROUP_RANGE;
        }
        return new FlexibleKeyGroupRange(new TreeMap<>(keyGroupRanges));
    }

}
