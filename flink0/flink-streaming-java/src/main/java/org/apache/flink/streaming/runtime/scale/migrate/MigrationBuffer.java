package org.apache.flink.streaming.runtime.scale.migrate;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

/**
 * This class is used to buffer the input data of the operator during the migration process,
 * which can not be processed temporarily due to related state migration not finished.
 * <p>
 * Notice this buffer is actually on-heap buffer,
 * so we should manage the buffer size by ourselves to avoid OOM.
 */
public class MigrationBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationBuffer.class);

    // may use one or both of the following two limits
    static final int MAX_RECORDS_NUM = ScaleConfig.Instance.CACHE_CAPACITY;
    int recordsNum = 0;

    private final List<InputChannelInfo> allInputChannels;
    private final Map<Integer, List<Integer>> inKeys = new HashMap<>();

    private Set<Integer> initByReroutedConfirm = new HashSet<>();
    private final Map<InputChannelInfo, Map<Integer,StreamBufferUnit>> channelMap = new HashMap<>();
    private final Map<Integer, Map<InputChannelInfo,StreamBufferUnit>> keyGroupMap = new HashMap<>();

    private final Set<Integer> mergedKeyGroups = new HashSet<>(); // keyGroups that have been merged to the local state


    public MigrationBuffer(IndexedInputGate[] inputGates){
        allInputChannels = new ArrayList<>();
        LOG.info("create MigrationBuffer with {} inputGates {}", inputGates.length, inputGates);
        for (IndexedInputGate gate : inputGates) {
            allInputChannels.addAll(gate.getChannelInfos());
        }
        LOG.info("create MigrationBuffer with inputChannels: {}", allInputChannels);
    }
    public void subscale(Map<Integer, List<Integer>> subInKeys){
        inKeys.putAll(subInKeys);
        subInKeys.forEach(
                (subtask, keyGroups) -> {
                    keyGroups.forEach(
                            keyGroup -> {
                                if (initByReroutedConfirm.contains(keyGroup)) {
                                    return;
                                }
                                // create StreamBufferUnit for each keyGroup and each channel
                                allInputChannels.forEach(
                                        channel -> {
                                            LOG.info("Create StreamBufferUnit for channel {} and keyGroup {}", channel, keyGroup);
                                            StreamBufferUnit unit = new StreamBufferUnit(channel, keyGroup);
                                            channelMap.computeIfAbsent(channel, k -> new HashMap<>()).put(keyGroup, unit);
                                            keyGroupMap.computeIfAbsent(keyGroup, k -> new HashMap<>()).put(channel, unit);
                                        }
                                );
                            }
                    );
                }
        );
    }


    public boolean isRemoteConfirmed(InputChannelInfo channelInfo, int keyGroupIndex) {
        StreamBufferUnit unit = channelMap.get(channelInfo).get(keyGroupIndex);
        return unit.remoteConfirmed;
    }

    /**
     * return true if the record is cached successfully,
     * otherwise return false, which means the buffer has reached the limit.
     * However, no matter true or false, current record will always be cached.
     */
    public <IN> boolean cacheRecord(StreamRecord<IN> record, InputChannelInfo channelInfo, int keyGroupIndex) {
        StreamBufferUnit unit = channelMap.get(channelInfo).get(keyGroupIndex);

        if (mergedKeyGroups.contains(keyGroupIndex) && unit.remoteConfirmed) {
            throw new IllegalStateException(
                    "The keyGroup has been merged to the local state and confirmed by the remote task, " +
                            "so it should not be cached in the buffer.");
        }

        if (unit.records == null || record == null) {
            LOG.error("unit {} records null: {}, record null: {}", unit, unit.records, record);
            throw new IllegalStateException("The buffer unit has been cleared, so it should not be cached in the buffer.");
        }

        unit.records.add(record);

        recordsNum++;
        // LOG.info("Cache record: {}/{} at {}", recordsNum, MAX_RECORDS_NUM, System.currentTimeMillis());
        return recordsNum < MAX_RECORDS_NUM;
    }

    /**
     * return true if the merge action do release some buffer space,
     */
    public boolean notifyStateMerged(
            Set<Integer> keyGroupRange,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordConsumer) {

        boolean notifyEmpty = (recordsNum == MAX_RECORDS_NUM);

        for(int keyGroupIndex : keyGroupRange){
            mergedKeyGroups.add(keyGroupIndex);
            keyGroupMap.get(keyGroupIndex).forEach((channelKey, unit) -> {
                    if (unit.remoteConfirmed) {

                        unit.records.forEach(
                            record -> {
                                try {
                                    recordConsumer.accept(record, channelKey);
                                    recordsNum--;
                                } catch (Exception e) {
                                    LOG.error("Failed to process record: {}", record, e);
                                }
                            }
                        );
                        unit.clear();
                        LOG.info("Unit {} cleared due to state merged, cache released: {}/{} at {}",
                                unit, recordsNum, MAX_RECORDS_NUM, System.currentTimeMillis());
                    }
                }
            );
        }
        return notifyEmpty && (recordsNum < MAX_RECORDS_NUM);
    }

    public boolean notifyRemoteConfirmed(
            InputChannelInfo channelInfo,
            Set<Integer> confirmedKeys,
            ScalingContext scalingContext,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordConsumer,
            int fromTaskIndex) {
        boolean notifyEmpty = (recordsNum == MAX_RECORDS_NUM);

        confirmedKeys.forEach(keyGroup ->{
            if (!keyGroupMap.containsKey(keyGroup)) {
                if (!ScaleConfig.Instance.ENABLE_SUBSCALE){
                    return;
                }
                // receive rerouted confirm barrier before trigger barrier
                LOG.info("KeyGroup {} is not in the buffer,"
                        + " may due to rerouted confirm barrier received before trigger barrier.", keyGroup);
                allInputChannels.forEach(
                        channel -> {
                            LOG.info("Create StreamBufferUnit for channel {} and keyGroup {}", channel, keyGroup);
                            StreamBufferUnit unit = new StreamBufferUnit(channel, keyGroup);
                            channelMap.computeIfAbsent(channel, k -> new HashMap<>()).put(keyGroup, unit);
                            keyGroupMap.computeIfAbsent(keyGroup, k -> new HashMap<>()).put(channel, unit);
                        }
                );
                initByReroutedConfirm.add(keyGroup);
            }

            final Map<InputChannelInfo, StreamBufferUnit> channelMap = keyGroupMap.get(keyGroup);
            final StreamBufferUnit unit = channelMap.get(channelInfo);
            unit.remoteConfirmed = true;
            if (mergedKeyGroups.contains(keyGroup)) {
                unit.records.forEach(
                        record -> {
                            try {
                                recordConsumer.accept(record, channelInfo);
                                recordsNum--;
                            } catch (Exception e) {
                                LOG.error("Failed to process record: {}", record, e);
                            }
                        }
                );
                unit.clear();
                LOG.info("Unit {} cleared due to remote confirmed, cache released: {}/{} at {}",
                        unit, recordsNum, MAX_RECORDS_NUM, System.currentTimeMillis());
            }
            boolean allConfirmed = channelMap.values().stream()
                    .allMatch(streamBufferUnit -> streamBufferUnit.remoteConfirmed);
            if(allConfirmed){
                scalingContext.notifyAllRemoteConfirmed(keyGroup,fromTaskIndex);
            }else{
                LOG.info("KeyGroup {} remote confirmed {}/{}",
                        keyGroup, channelMap.values().stream().filter(streamBufferUnit -> streamBufferUnit.remoteConfirmed).count(), channelMap.size());
            }

        });


        return notifyEmpty && (recordsNum < MAX_RECORDS_NUM);
    }


    private static class StreamBufferUnit{
        List<StreamRecord> records;
        InputChannelInfo channelInfo;
        int keyGroupIndex;

        boolean remoteConfirmed = false;

        StreamBufferUnit(InputChannelInfo channelInfo, int keyGroupIndex){
            this.channelInfo = channelInfo;
            this.keyGroupIndex = keyGroupIndex;
            records = new ArrayList<>();
        }
        void clear(){
            records = null;
        }
        @Override
        public String toString() {
            return "StreamBufferUnit[" + channelInfo + "," + keyGroupIndex + "]";
        }
    }

}
