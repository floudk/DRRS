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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is used to buffer the input data of the operator during the migration process,
 * which can not be processed temporarily due to related state migration not finished.
 * <p>
 * Notice this buffer is actually on-heap buffer,
 * so we should manage the buffer size by ourselves to avoid OOM.
 */
public class MigrationBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationBuffer.class);

    static final int MAX_RECORDS_NUM = ScaleConfig.Instance.CACHE_CAPACITY;

    volatile int recordsNum = 0;
    Map<Integer, Integer> keyCounts = new ConcurrentHashMap<>();
    private final Map<Integer, WaitingTimeStats> keyWaitingStats = new ConcurrentHashMap<>();

    private final List<InputChannelInfo> allInputChannels;
    private final Map<Integer, List<Integer>> inKeys = new HashMap<>();

    private final ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordConsumer;
    private Set<Integer> initByReroutedConfirm = new HashSet<>();
    private final Map<InputChannelInfo, Map<Integer, BufferUnit>> channelMap = new HashMap<>();
    private final Map<Integer, Map<InputChannelInfo, BufferUnit>> keyGroupMap = new HashMap<>();

    private final Set<Integer> mergedKeyGroups = new HashSet<>(); // keyGroups that have been merged to the local state


    public MigrationBuffer(
            IndexedInputGate[] inputGates,
            ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> recordConsumer) {
        this.allInputChannels = Arrays.stream(inputGates)
                .flatMap(gate -> gate.getChannelInfos().stream())
                .collect(Collectors.toList());
        this.recordConsumer = recordConsumer;
        LOG.info("Created MigrationBuffer with {} inputGates, channels: {}", inputGates.length, allInputChannels);

    }
    public void subscale(Map<Integer, List<Integer>> subInKeys){
        inKeys.putAll(subInKeys);
        subInKeys.forEach((subtask, keyGroups) ->
                keyGroups.stream()
                        .filter(keyGroup -> !initByReroutedConfirm.contains(keyGroup))
                        .forEach(this::createBufferUnitsForKeyGroup)
        );
    }

    private void createBufferUnitsForKeyGroup(int keyGroup) {
        allInputChannels.forEach(channel -> {
            LOG.info("Creating BufferUnit for channel {} and keyGroup {}", channel, keyGroup);
            BufferUnit unit = new BufferUnit(channel, keyGroup);
            channelMap.computeIfAbsent(channel, k -> new HashMap<>()).put(keyGroup, unit);
            keyGroupMap.computeIfAbsent(keyGroup, k -> new HashMap<>()).put(channel, unit);
        });
    }

    public boolean isRemoteConfirmed(InputChannelInfo channelInfo, int keyGroupIndex) {
        BufferUnit unit = channelMap.get(channelInfo).get(keyGroupIndex);
        return unit.remoteConfirmed;
    }

    /**
     * return true if the record is cached successfully,
     * otherwise return false, which means the buffer has reached the limit.
     * However, no matter true or false, current record will always be cached.
     */
    public <IN> boolean cacheRecord(StreamRecord<IN> record, InputChannelInfo channelInfo, int keyGroupIndex) {
        BufferUnit unit = channelMap.get(channelInfo).get(keyGroupIndex);

        // validateCacheOperation
        if (unit.records == null || record == null) {
            LOG.error("unit {} records null: {}, record null: {}", unit, unit.records, record);
            throw new IllegalStateException("The buffer unit has been cleared, so it should not be cached in the buffer.");
        }
        if (mergedKeyGroups.contains(keyGroupIndex) && unit.remoteConfirmed) {
            throw new IllegalStateException(
                    "The keyGroup has been merged to the local state and confirmed by the remote task, " +
                            "so it should not be cached in the buffer.");
        }


        unit.records.add(record);
        recordsNum++;
        keyCounts.merge(keyGroupIndex, 1, Integer::sum);
        keyWaitingStats.computeIfAbsent(keyGroupIndex, k -> new WaitingTimeStats())
                .recordArrival();

        // LOG.info("Cache record: {}/{} at {}", recordsNum, MAX_RECORDS_NUM, System.currentTimeMillis());
        return recordsNum < MAX_RECORDS_NUM;
    }

    private void processConfirmedUnit(int keyGroupIndex, InputChannelInfo inputChannel, BufferUnit unit ) {
        int unitCount = unit.records.size();
        if (unitCount == 0){
            unit.clear();
            return;
        }
        unit.records.forEach(
                record -> {
                    try {
                        recordConsumer.accept(record, inputChannel);
                        keyWaitingStats.get(keyGroupIndex).recordConsumption();
                    } catch (Exception e) {
                        LOG.error("Failed to process record: {}", record, e);
                    }
                }
        );
        recordsNum -= unitCount;
        keyCounts.put(keyGroupIndex, keyCounts.get(keyGroupIndex) - unitCount);
        unit.clear();
//        LOG.info("Unit {} cleared due to state merged, cache released: {}/{} at {}",
//                unit, recordsNum, MAX_RECORDS_NUM, System.currentTimeMillis());
    }
    /**
     * return true if the merge action do release some buffer space,
     */
    public boolean notifyStateMerged(Set<Integer> keyGroupRange) {
        boolean wasFullBeforeMerge = (recordsNum == MAX_RECORDS_NUM);

        for(int keyGroupIndex : keyGroupRange){
            mergedKeyGroups.add(keyGroupIndex);
            keyGroupMap.get(keyGroupIndex).forEach((channelKey, unit) -> {
                    if (unit.remoteConfirmed) {
                        processConfirmedUnit(keyGroupIndex, channelKey, unit);
                    }
                }
            );
        }

        return wasFullBeforeMerge && (recordsNum < MAX_RECORDS_NUM);
    }

    public boolean notifyStateMergedWithDisabledDR(
            Set<Integer> keyGroupRange,
            ScalingContext scalingContext,
            int fromTaskIndex) {
        checkState(!ScaleConfig.Instance.ENABLE_DR, "notifyAllRemoteConfirmed should not be called in DR mode");
        boolean notifyEmpty = (recordsNum == MAX_RECORDS_NUM);

        for(int keyGroupIndex : keyGroupRange){
            mergedKeyGroups.add(keyGroupIndex);
            keyGroupMap.get(keyGroupIndex).forEach((channelKey, unit) -> {
                if (unit.records == null){
                    LOG.error("unit {} records is null", unit);
                    throw new IllegalStateException("The buffer unit has been cleared, so it should not be cached in the buffer.");
                }
                processConfirmedUnit(keyGroupIndex, channelKey, unit);
            });
            scalingContext.notifyAllRemoteConfirmed(keyGroupIndex,fromTaskIndex);
        }
        return notifyEmpty && (recordsNum < MAX_RECORDS_NUM);
    }

    public boolean notifyRemoteConfirmed(
            InputChannelInfo channelInfo,
            Set<Integer> confirmedKeys,
            ScalingContext scalingContext,
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
                            //LOG.info("Create StreamBufferUnit for channel {} and keyGroup {}", channel, keyGroup);
                            BufferUnit unit = new BufferUnit(channel, keyGroup);
                            channelMap.computeIfAbsent(channel, k -> new HashMap<>()).put(keyGroup, unit);
                            keyGroupMap.computeIfAbsent(keyGroup, k -> new HashMap<>()).put(channel, unit);
                        }
                );
                initByReroutedConfirm.add(keyGroup);
            }

            final Map<InputChannelInfo, BufferUnit> channelMap = keyGroupMap.get(keyGroup);
            final BufferUnit unit = channelMap.get(channelInfo);
            unit.remoteConfirmed = true;
            if (mergedKeyGroups.contains(keyGroup)) {
                processConfirmedUnit(keyGroup, channelInfo, unit);
            }
            boolean allConfirmed = channelMap.values().stream()
                    .allMatch(bufferUnit -> bufferUnit.remoteConfirmed);
            if(allConfirmed){
                scalingContext.notifyAllRemoteConfirmed(keyGroup,fromTaskIndex);
            }else{
                LOG.info("KeyGroup {} remote confirmed {}/{}",
                        keyGroup, channelMap.values().stream().filter(bufferUnit -> bufferUnit.remoteConfirmed).count(), channelMap.size());
            }
        });


        return notifyEmpty && (recordsNum < MAX_RECORDS_NUM);
    }


    private static class BufferUnit {
        List<StreamRecord> records;
        InputChannelInfo channelInfo;
        int keyGroupIndex;
        boolean remoteConfirmed = false;

        BufferUnit(InputChannelInfo channelInfo, int keyGroupIndex){
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

    private static class WaitingTimeStats {
        private final Queue<Long> arrivalTimes = new ConcurrentLinkedQueue<>();

        void recordArrival() {
            arrivalTimes.offer(System.currentTimeMillis());
        }

        void recordConsumption() {
            arrivalTimes.poll();
        }
        double getCurrentAverageWaitingTime() {
            long currentTime = System.currentTimeMillis();
            return arrivalTimes.stream()
                    .mapToLong(arrivalTime -> currentTime - arrivalTime)
                    .average()
                    .orElse(0);
        }
    }


    public void close(){
        keyCounts.clear();
        allInputChannels.clear();
        inKeys.clear();
        initByReroutedConfirm.clear();
        channelMap.clear();
        keyGroupMap.clear();
        mergedKeyGroups.clear();
    }

    // --------------------------- scheduling -----------------------------------
    // async-thread
    public boolean checkEmergency(int currentKeyGroup) {
        // exceed the pre-defined thread
        double currentRatio;
        if (keyCounts.containsKey(currentKeyGroup)) {
            currentRatio = (double) (recordsNum - keyCounts.get(currentKeyGroup)) / MAX_RECORDS_NUM;
        } else {
            currentRatio = (double) recordsNum / MAX_RECORDS_NUM;
        }
        LOG.info("Emergency check: {}/{} = {}({})", recordsNum, MAX_RECORDS_NUM, currentRatio, ScaleConfig.Instance.MIGRATION_BUFFER_EMERGENCY_RATIO);
        return currentRatio > ScaleConfig.Instance.MIGRATION_BUFFER_EMERGENCY_RATIO;
    }

    // async-thread
    // if the key is in the buffer and not 0, add the count
    public Map<Integer, Integer> getKeyCounts(Set<Integer> needKeys) {
        return needKeys.stream()
                .collect(Collectors.toMap(
                        key -> key,
                        key -> keyCounts.getOrDefault(key, 0)
                ));
    }

    public Map<Integer, Double> getAverageWaitingTime(Set<Integer> needKeys) {
        return needKeys.stream()
                .collect(Collectors.toMap(
                        key -> key,
                        key -> keyWaitingStats.containsKey(key)
                                ? keyWaitingStats.get(key).getCurrentAverageWaitingTime()
                                : 0.0
                ));
    }

}
