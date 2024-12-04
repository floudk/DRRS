package org.apache.flink.runtime.scale.io.message.local;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.state.heap.StateMapSnapshot;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * un-serialized state buffer.
 * used for local state buffer transfer(not completely supported yet),
 * and for storing related state buffer information before serialization.
 */
public class StateBuffer<K, N, S> extends ScaleBuffer {

    public String stateName;
    public Map<Integer, StateMap<K, N, S>> outgoingManagedKeyedState;



    public StateBuffer() {

    }

    public StateBuffer(
            String stateName,
            Map<Integer, StateMap<K, N, S>> outgoingManagedKeyedState) {
        this.stateName = stateName;
        this.outgoingManagedKeyedState = outgoingManagedKeyedState;
    }

    public void setEmpty() {
        this.stateName = "empty";
        this.outgoingManagedKeyedState = null;
    }

    public boolean isEmpty() {
        return this.stateName.equals("empty") && this.outgoingManagedKeyedState == null;
    }

    public StateBufferDelegate toStateBufferDelegate(
            ByteBufAllocator allocator,
            Function<String, Tuple3<TypeSerializer<K>, TypeSerializer<N>, TypeSerializer<S>>>
                    localKNSSerializerSupplier,
            Function<String, StateSnapshotTransformer<S>> snapshotTransformerSupplier) {

        ByteBuf bb = allocator.buffer( stateName.getBytes().length + 4 + 4096);
        try (ByteBufOutputStream bbos = new ByteBufOutputStream(bb)) {


            DataOutputViewStreamWrapper dataOutput = new DataOutputViewStreamWrapper(bbos);

            writeString(bb, stateName); // 4 + stateName.getBytes().length
            bb.writeInt(outgoingManagedKeyedState.size()); // 4

            Tuple3<TypeSerializer<K>, TypeSerializer<N>, TypeSerializer<S>> serializerTuple = localKNSSerializerSupplier.apply(
                    stateName);
            StateSnapshotTransformer<S> stateSnapshotTransformer = snapshotTransformerSupplier.apply(
                    stateName);
            for (Map.Entry<Integer, StateMap<K, N, S>> entry : outgoingManagedKeyedState.entrySet()) {
                LOG.info("Serializing StateBuffer on {}", entry.getKey());
                dataOutput.writeInt(entry.getKey());
                StateMapSnapshot snapshot = entry.getValue().stateSnapshot();
                snapshot.writeState(
                        serializerTuple.f0.duplicate(),
                        serializerTuple.f1.duplicate(),
                        serializerTuple.f2.duplicate(),
                        dataOutput,
                        stateSnapshotTransformer);
            }
            LOG.info("StateBuffer created on {} with size {} KB", outgoingManagedKeyedState.keySet(), bb.readableBytes() / 1024);
        } catch (Exception e) {
            LOG.error("Failed to serialize StateBuffer", e);
            bb.release();
            throw new RuntimeException(e);
        }
        return new StateBufferDelegate(bb);
    }

    @Override
    public int getSize() {
        throw new RuntimeException("StateBuffer should not be written to ByteBuf directly");
    }

    @Override
    public ByteBuf write(
            ByteBufAllocator allocator,
            byte ID,
            JobVertexID jobVertexID,
            int sender,
            int receiver) {
        throw new RuntimeException("StateBuffer should not be written to ByteBuf directly");
    }

    @Override
    public String toString() {
        return "[" + stateName + "->" + outgoingManagedKeyedState.keySet() + "]";
    }

    public void setTimerServiceStateFuture(Consumer<ByteBuf> timerServiceStateFuture) {
        timerServiceStateFuture.accept(null);
    }

    public static class TimerServiceState{
        String serviceName;
        ByteBuf state;
        public TimerServiceState(String serviceName, ByteBuf state){
            this.serviceName = serviceName;
            this.state = state;
        }
    }
}