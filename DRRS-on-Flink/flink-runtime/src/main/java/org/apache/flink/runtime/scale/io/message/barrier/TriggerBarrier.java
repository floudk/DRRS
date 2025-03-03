package org.apache.flink.runtime.scale.io.message.barrier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.scale.io.SubscaleTriggerInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TriggerBarrier extends ScaleBarrier{
    private static final long serialVersionUID = 1L;

    public final Map<Integer, Long> involvedKeys; // key group index involved in current subscale
    // source task index -> [target task index -> key groups]
    private final Map<Integer, Map<Integer, List<Integer>>> outMap;
    // target task index -> [source task index -> key groups]
    private final Map<Integer, Map<Integer, List<Integer>>> inMap;

    public final int subscaleID;

    public TriggerBarrier(Map<Integer, SubscaleTriggerInfo> involvedInfos, Map<Integer, Integer> currentKeyPartition, int subscaleID){
        super();
        List<Integer> involvedKeys = new ArrayList<>(involvedInfos.keySet());
        Map<Integer, Tuple2<Integer,Integer>> keyGroupSourceWithTarget = new HashMap<>();
        involvedKeys.forEach(key ->
                keyGroupSourceWithTarget.put(
                        key, new Tuple2<>(currentKeyPartition.get(key), involvedInfos.get(key).newPartitioningPos)));

        Map<Integer, Map<Integer, List<Integer>>> outMap = new HashMap<>();
        Map<Integer, Map<Integer, List<Integer>>> inMap = new HashMap<>();
        keyGroupSourceWithTarget.forEach((key, tuple) -> {
            int source = tuple.f0;
            int target = tuple.f1;
            outMap.putIfAbsent(source, new HashMap<>());
            outMap.get(source).putIfAbsent(target, new ArrayList<>());
            outMap.get(source).get(target).add(key);

            inMap.putIfAbsent(target, new HashMap<>());
            inMap.get(target).putIfAbsent(source, new ArrayList<>());
            inMap.get(target).get(source).add(key);
        });
        this.involvedKeys = new HashMap<>();
        involvedKeys.forEach(key -> this.involvedKeys.put(key, involvedInfos.get(key).stateSize));

        this.outMap = outMap;
        this.inMap = inMap;
        this.subscaleID = subscaleID;
    }

    @Override
    public String toString(){return "TB(" + subscaleID + ")";}

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TriggerBarrier) {
            TriggerBarrier other = (TriggerBarrier) obj;
            return subscaleID == other.subscaleID;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return subscaleID;
    }


    public List<Integer> getInvolvedSubpartitions() {
        // get involved subpartitions: source + target
        Set<Integer> involvedSubpartitions = new HashSet<>(inMap.keySet());
        involvedSubpartitions.addAll(outMap.keySet());
        return new ArrayList<>(involvedSubpartitions);
    }

    public Map<Integer, List<Integer>> getInKeys(int subtaskIndex) {
        return inMap.getOrDefault(subtaskIndex, Collections.emptyMap());
    }

    public Map<Integer, List<Integer>> getOutKeys(int subtaskIndex) {
        return outMap.getOrDefault(subtaskIndex, Collections.emptyMap());
    }

    public static ByteBuffer serializeTriggerBarrier(TriggerBarrier barrier) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(barrier);
            out.flush();
        }

        int capacity = 4 + bos.size(); // barrier type(4) + serialized object
        ByteBuffer buf = ByteBuffer.allocate(capacity);

        buf.putInt(EventSerializer.SCALE_TRIGGER_EVENT); // barrier type
        buf.put(bos.toByteArray()); // serialized object

        buf.flip();
        return buf;
    }
    public static TriggerBarrier deserializeTriggerBarrier(ByteBuffer buffer) throws IOException {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        try (ObjectInputStream in = new ObjectInputStream(bis)) {
            return (TriggerBarrier) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
