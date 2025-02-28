package org.apache.flink.runtime.scale.io.message;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.network.netty.NettyMessage;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.state.KeyOrStateID;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.state.heap.StateMapSnapshot;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.flink.runtime.scale.io.network.ScaleNettyMessage.ScaleNettyMessage_HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


public abstract class ScaleBuffer {
    protected static final Logger LOG = LoggerFactory.getLogger(ScaleBuffer.class);

    public static ScaleBuffer fromByteBuf(ByteBuf in) {
        byte id = in.readByte();
        switch (id) {
            case StateBufferDelegate.BTYPE:
                return StateBufferDelegate.fromByteBuf(in);
            default:
                throw new IllegalArgumentException("Unknown ScaleBuffer type: " + id);
        }
    }

    public static String readString(ByteBuf buffer) {
        int length = buffer.readInt();
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return new String(bytes);
    }

    public static void writeString(ByteBuf bb, String value) {
        byte[] bytes = value.getBytes();
        bb.writeInt(bytes.length);
        bb.writeBytes(bytes);
    }

    public abstract int getSize();

    public abstract ByteBuf write(
            ByteBufAllocator allocator, byte ID, JobVertexID jobVertexID, int sender, int receiver);


    public static class StateBufferDelegate extends ScaleBuffer{

        protected static final byte BTYPE = 1;

        private final ByteBuf delegate;

        public StateBufferDelegate(ByteBuf delegate) {
            this.delegate = delegate;
        }

        @Override
        public ByteBuf write(
                ByteBufAllocator allocator, byte ID, JobVertexID jobVertexID, int sender, int receiver) {
            CompositeByteBuf compositeByteBuf = allocator.compositeBuffer();
            ByteBuf header = NettyMessage.allocateBuffer(
                    allocator,
                    ID,
                    ScaleNettyMessage_HEADER_LENGTH + 5, // 4 for bufferID, 1 for buffer type
                    getSize() - 5,
                    false);
            jobVertexID.writeTo(header);
            header.writeInt(sender);
            header.writeInt(receiver);

            header.writeByte(BTYPE); // buffer type
            compositeByteBuf.addComponent(true, header);

            // use slice to avoid increasing the reference count
            // when the compositeByteBuf is released, the delegate will be released as well
            compositeByteBuf.addComponent(true, delegate.slice());
            return compositeByteBuf;
        }

        @Override
        public int getSize() {
            // 4 for bufferID 1 for buffer type
            return delegate.readableBytes() + 1;
        }

        public static StateBufferDelegate fromByteBuf(ByteBuf in) {
            // retain the buffer to prevent it from being released
            return new StateBufferDelegate(in.retain());
        }


        // deserialize the state related information from ByteBuf
        public <K,N,S> StateBuffer toStateBuffer(
                BiFunction<String, BitSet, StateMap> stateMapCreator,
                Function<String, Tuple3<TypeSerializer,TypeSerializer,TypeSerializer>>
                        typeSerializerGetter) throws IOException {

            String stateName = readString(delegate);

            Map<KeyOrStateID, StateMap> stateMaps = new HashMap<>();
            int numberOfStateMaps = delegate.readInt();

            BitSet binFlags = null;
            byte hasBinFlags = delegate.readByte();
            if (hasBinFlags == 1) {
                int binFlagsLength = delegate.readInt();
                byte[] binFlagsBytes = new byte[binFlagsLength];
                delegate.readBytes(binFlagsBytes);
                binFlags = BitSet.valueOf(binFlagsBytes);
            }

            Tuple3<TypeSerializer,TypeSerializer,TypeSerializer> typeSerializers =
                    typeSerializerGetter.apply(stateName);

            TypeSerializer<K> keySerializer = typeSerializers.f0.duplicate();
            TypeSerializer<N> namespaceSerializer = typeSerializers.f1.duplicate();
            TypeSerializer<S> valueSerializer = typeSerializers.f2.duplicate();

            InputStream inputStream = new ByteBufInputStream(delegate);
            DataInputView div = new DataInputViewStreamWrapper(inputStream);

            checkState(numberOfStateMaps == 1, "Only support one state map in Meces");

            for(int i = 0; i < numberOfStateMaps; i++) {
                KeyOrStateID keyOrStateID = KeyOrStateID.readFrom(div);
                if (keyOrStateID.isKey()){
                    checkNotNull(binFlags, "binFlags should not be null when sending state by key");
                }

                StateMap stateMap = stateMapCreator.apply(stateName, binFlags);
                int numEntries = div.readInt();
                for (int j = 0; j < numEntries; j++) {
                    N namespace = namespaceSerializer.deserialize(div);
                    K key = keySerializer.deserialize(div);
                    S value = valueSerializer.deserialize(div);
                    stateMap.put(key, namespace, value);
                }
                stateMaps.put(keyOrStateID, stateMap);
            }

            delegate.release(); // release the buffer after reading as receiver
            StateBuffer stateBuffer = new StateBuffer(stateName, stateMaps);
            stateBuffer.binFlags = binFlags;
            return stateBuffer;
        }
    }

    public static class StateBuffer<K,N,S> extends ScaleBuffer{

        public final String stateName;
        public final Map<KeyOrStateID, StateMap<K, N, S>> outgoingManagedKeyedState;

        @Nullable
        public BitSet binFlags;

        // empty state buffer
        public static final StateBuffer EMPTY_STATE_BUFFER =
                new StateBuffer("empty", null);

        public StateBuffer(String stateName,
                    Map<KeyOrStateID, StateMap<K, N, S>> outgoingManagedKeyedState) {
            this.stateName = stateName;
            this.outgoingManagedKeyedState = outgoingManagedKeyedState;
        }

        public ScaleBuffer.StateBufferDelegate toStateBufferDelegate(
                ByteBufAllocator allocator,
                Function<String, Tuple3<TypeSerializer<K>,TypeSerializer<N>,TypeSerializer<S>>>
                        localKNSSerializerSupplier,
                Function<String,StateSnapshotTransformer<S>> snapshotTransformerSupplier) {

            // Allocate a new ByteBuf to hold all the data
            // however, we don't know the length of the data yet
            // so we just allocate a ByteBuf with a default size: 1024
            // and rely on the auto-expansion of the ByteBuf
            ByteBuf bb = allocator.buffer(
                    4 + stateName.getBytes().length // writeString
                            + 4 + 1  // stateSize + binFlags(is null)
                            + 4096); // write outgoingManagedKeyedState(including possible binFlags)
            // print available memory in allocator

            try (ByteBufOutputStream bbos = new ByteBufOutputStream(bb)){
                DataOutputViewStreamWrapper dataOutput = new DataOutputViewStreamWrapper(bbos);
                writeString(bb, stateName); // 4 + stateName.getBytes().length
                bb.writeInt(outgoingManagedKeyedState.size()); // 4
                bb.writeByte(binFlags == null ? 0 : 1); // 1
                if (binFlags != null) {
                    byte[] binFlagsBytes = binFlags.toByteArray();
                    bb.writeInt(binFlagsBytes.length);
                    bb.writeBytes(binFlagsBytes);
                }
                Tuple3<TypeSerializer<K>, TypeSerializer<N>, TypeSerializer<S>> serializerTuple = localKNSSerializerSupplier.apply(stateName);
                StateSnapshotTransformer<S> stateSnapshotTransformer = snapshotTransformerSupplier.apply(stateName);
                for (Map.Entry<KeyOrStateID, StateMap<K, N, S>> entry : outgoingManagedKeyedState.entrySet()) {
                    entry.getKey().writeTo(dataOutput);
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
                bb.release();
                LOG.error("Failed to serialize StateBuffer", e);
                throw new RuntimeException(e);
            }
            return new ScaleBuffer.StateBufferDelegate(bb);
        }

        @Override
        public int getSize() {
            throw new RuntimeException("StateBuffer should not be written to ByteBuf directly");
        }
        @Override
        public ByteBuf write(
                ByteBufAllocator allocator, byte ID, JobVertexID jobVertexID, int sender, int receiver) {
            throw new RuntimeException("StateBuffer should not be written to ByteBuf directly");
        }

        @Override
        public String toString() {
            return "[" + stateName + "->" + outgoingManagedKeyedState.keySet() + "]";
        }
    }

}
