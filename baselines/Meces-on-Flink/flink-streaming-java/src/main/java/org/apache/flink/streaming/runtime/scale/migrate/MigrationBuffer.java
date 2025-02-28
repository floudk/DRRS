package org.apache.flink.streaming.runtime.scale.migrate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.state.HierarchicalStateID;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is used to buffer the input data of the operator during the migration process,
 * which can not be processed temporarily due to related state migration not finished.
 * <p>
 * Notice this buffer is actually on-heap buffer,
 * so we should manage the buffer size by ourselves to avoid OOM.
 */

public class MigrationBuffer {

//    static final int MAX_RECORDS_NUM = ScaleConfig.Instance.CACHE_CAPACITY;
//    private final LinkedList<CacheEntry> cache = new LinkedList<>();

    StreamRecord cachedRecord = null;
    InputChannelInfo cachedChannelInfo = null;
    HierarchicalStateID cachedStateID = null;

    public boolean fetched = true;

    public HierarchicalStateID getCachedBinID() {
        return cachedStateID;
//        if (cache.isEmpty()) {
//            return null;
//        }
//        return cache.getFirst().stateID;
    }

    /**
     * return true if the record is cached successfully,
     * otherwise return false, which means the buffer has reached the limit.
     * However, no matter true or false, current record will always be cached.
     */
    public <IN> void cacheRecord(
            StreamRecord<IN> record, InputChannelInfo channelInfo, HierarchicalStateID stateID) {
        checkState(cachedRecord == null, "The previous record has not been processed yet.");
        cachedRecord = record;
        cachedChannelInfo = channelInfo;
        cachedStateID = stateID;
//        cache.add(new CacheEntry(record, stateID));
    }

    /**
     * return true if the merge action do release some buffer space.
     * Running in main thread.
     */
    public boolean notifyStateOrBinMerged(
            ScalingContext scalingContext,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordConsumer) throws Exception {
        if (cachedRecord == null) {
            return false;
        }
        if (scalingContext.isLocalBin(cachedStateID)) {
            recordConsumer.accept(cachedRecord, cachedChannelInfo);
            cachedRecord = null;
            cachedChannelInfo = null;
            cachedStateID = null;
            return true;
        }
        return false;
//        if (cache.isEmpty()) {
//            return false;
//        }
//
//        int preSize = cache.size();

        // release as many records as possible, eager release
//        while(!cache.isEmpty()){
//            CacheEntry entry = cache.getFirst();
//            if (scalingContext.isLocalBin(entry.stateID)) {
//                recordConsumer.accept(entry.record, null);
//                cache.removeFirst();
//            } else {
//                break;
//            }
//        }

         // only release the first record, lazy release
//        CacheEntry entry = cache.getFirst();
//        if(scalingContext.isLocalBin(entry.stateID)){
//            recordConsumer.accept(entry.record, null);
//            cache.removeFirst();
//        }
//
//        if (!cache.isEmpty() &&  cache.size() < preSize) {
//            fetched = false;
//        }
//
//        return preSize == MAX_RECORDS_NUM &&  cache.size() < MAX_RECORDS_NUM;
    }

//    public boolean isEmpty() {
//        return cache.isEmpty();
//    }
//
//    public boolean isFull() {
//        return cache.size() >= MAX_RECORDS_NUM;
//    }

//    public Tuple2<StreamRecord, HierarchicalStateID> peek() {
//        if (cache.isEmpty()) {
//            return null;
//        }
//        CacheEntry entry = cache.getFirst();
//        return Tuple2.of(entry.record, entry.stateID);
//    }
//    public void poll() {
//        cache.removeFirst();
//    }
//
//
//    private static class CacheEntry {
//        StreamRecord record;
//        HierarchicalStateID stateID;
//        CacheEntry(StreamRecord record, HierarchicalStateID stateID){
//            this.record = record;
//            this.stateID = stateID;
//        }
//    }
}
