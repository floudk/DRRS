package org.apache.flink.runtime.scale.io.message.barrier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

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

    public final Set<JobVertexID> upstreamJobVertexIDs;
    public final JobVertexID scalingJobVertexID;

    // --------------- null when the task is involved in the scaling ---------------
    @Nullable
    public Map<Integer, Integer> newKeyPartition;

    // --------------- null when the task is not involved in the scaling ---------------
    @Nullable
    private List<Integer> involvedKeys;
    // source task index -> [target task index -> key groups]

    @Nullable
    private Map<Integer, Map<Integer, List<Integer>>> outMap;

    // target task index -> [source task index -> key groups]
    @Nullable
    private Map<Integer, Map<Integer, List<Integer>>> inMap;

    // for the task that is not involved in the scaling
    public TriggerBarrier(
            JobVertexID scalingJobVertexID,
            Set<JobVertexID> upstreamJobVertexIDs,
            Map<Integer,Integer> newKeyPartition){
        super();
        this.scalingJobVertexID = scalingJobVertexID;
        this.upstreamJobVertexIDs = upstreamJobVertexIDs;
        this.newKeyPartition = newKeyPartition;
    }

    public TriggerBarrier(
            JobVertexID scalingJobVertexID,
            Set<JobVertexID> upstreamJobVertexIDs,
            Map<Integer,Integer> newKeyPartition,
            Map<Integer, Integer> currentKeyPartition){
        super();

        this.scalingJobVertexID = scalingJobVertexID;
        this.upstreamJobVertexIDs = upstreamJobVertexIDs;

        List<Integer> involvedKeys = new ArrayList<>(newKeyPartition.keySet());
        Map<Integer, Tuple2<Integer,Integer>> keyGroupSourceWithTarget = new HashMap<>();
        involvedKeys.forEach(key ->
                keyGroupSourceWithTarget.put(
                        key, new Tuple2<>(currentKeyPartition.get(key), newKeyPartition.get(key))));

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
        this.involvedKeys = Collections.unmodifiableList(involvedKeys);
        this.outMap = outMap;
        this.inMap = inMap;
    }

    @Override
    public String toString(){return "Trigger Barrier";}

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TriggerBarrier) {
            TriggerBarrier other = (TriggerBarrier) obj;
            return scalingJobVertexID.equals(other.scalingJobVertexID);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return scalingJobVertexID.hashCode();
    }


    public Map<Integer, List<Integer>> getInKeys(int subtaskIndex) {
        return inMap.getOrDefault(subtaskIndex, Collections.emptyMap());
    }

    public Map<Integer, List<Integer>> getOutKeys(int subtaskIndex) {
        return outMap.getOrDefault(subtaskIndex, Collections.emptyMap());
    }

    public Set<Integer> getInvolvedKeys() {
        return new HashSet<>(involvedKeys);
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
