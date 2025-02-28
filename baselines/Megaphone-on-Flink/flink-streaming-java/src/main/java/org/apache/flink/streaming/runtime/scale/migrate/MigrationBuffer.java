package org.apache.flink.streaming.runtime.scale.migrate;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is used to buffer the input data of the operator during the migration process,
 * which can not be processed temporarily due to related state migration not finished.
 * <p>
 * Notice this buffer is actually on-heap buffer,
 * so we should manage the buffer size by ourselves to avoid OOM.
 */
public class MigrationBuffer {

    static final int MAX_RECORDS_NUM = ScaleConfig.Instance.CACHE_CAPACITY;
    private final LinkedList<CacheEntry> cache = new LinkedList<>();

//    StreamRecord cachedRecord = null;
//    InputChannelInfo cachedChannelInfo = null;
//    int cachedKeyGroupIndex = -1;


    /**
     * return true if the record is cached successfully,
     * otherwise return false, which means the buffer has reached the limit.
     * However, no matter true or false, current record will always be cached.
     */
    public <IN> boolean cacheRecord(
            StreamRecord<IN> record,
            InputChannelInfo channelInfo,
            int keyGroupIndex) {
//        checkState(cachedRecord == null, "The previous record has not been processed yet.");
//        cachedRecord = record;
//        cachedChannelInfo = channelInfo;
//        cachedKeyGroupIndex = keyGroupIndex;
        cache.add(new CacheEntry(record, keyGroupIndex));
        return cache.size() < MAX_RECORDS_NUM;
    }

    /**
     * return true if the merge action do release some buffer space,
     */
    public boolean notifyStateMerged(
            ScalingContext scalingContext,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordConsumer) throws Exception {
//        if (cachedRecord == null) {
//            return false;
//        }
        if (cache.isEmpty()) {
            return false;
        }

        while(!cache.isEmpty()){
            CacheEntry firstEntry = cache.get(0);
            if (scalingContext.isIncomingKey(firstEntry.keyGroupIndex)){
                break;
            }
            recordConsumer.accept(firstEntry.record, null);
            cache.removeFirst();
        }

//        CacheEntry firstEntry = cache.get(0);

//        for(int keyGroupIndex : keyGroupRange){
////            if (keyGroupIndex == cachedKeyGroupIndex){
////                recordConsumer.accept(cachedRecord, cachedChannelInfo);
////                cachedRecord = null;
////                cachedChannelInfo = null;
////                cachedKeyGroupIndex = -1;
////                return true;
////            }
//            if (keyGroupIndex == firstEntry.keyGroupIndex){
//                recordConsumer.accept(firstEntry.record, firstEntry.channelInfo);
//                cache.remove(0);
//                return true;
//            }
//        }
        return cache.size() < MAX_RECORDS_NUM;
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    private static class CacheEntry {
        StreamRecord record;
        int keyGroupIndex;
        CacheEntry(StreamRecord record, int keyGroupIndex){
            this.record = record;
            this.keyGroupIndex = keyGroupIndex;
        }
    }


}
