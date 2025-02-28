package org.apache.flink.runtime.scale.io.message;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.io.network.netty.NettyMessage;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.io.message.local.StateBuffer;
import org.apache.flink.runtime.state.heap.StateMap;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.runtime.scale.io.network.ScaleNettyMessage.ScaleNettyMessage_HEADER_LENGTH;


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

        public final int bufferID;
        private final ByteBuf delegate;

        public StateBufferDelegate(ByteBuf delegate, int bufferID) {
            this.delegate = delegate;
            this.bufferID = bufferID;
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
            header.writeInt(bufferID); // buffer ID
            compositeByteBuf.addComponent(true, header);

            // use slice to avoid increasing the reference count
            // when the compositeByteBuf is released, the delegate will be released as well
            compositeByteBuf.addComponent(true, delegate.slice());
            return compositeByteBuf;
        }

        @Override
        public int getSize() {
            // 4 for bufferID 1 for buffer type
            return delegate.readableBytes() + 4 + 1;
        }

        public static StateBufferDelegate fromByteBuf(ByteBuf in) {
            int bufferID = in.readInt();
            // retain the buffer to prevent it from being released
            return new StateBufferDelegate(in.retain(), bufferID);
        }


        // deserialize the state related information from ByteBuf
        public  <K,N,S> StateBuffer toStateBuffer(
                Function<String, StateMap> stateMapGetter,
                Function<String, Tuple3<TypeSerializer,TypeSerializer,TypeSerializer>>
                        typeSerializerGetter) throws IOException {
            String stateName = readString(delegate);

            Map<Integer, StateMap> stateMaps = new HashMap<>();

            int numberOfStateMaps = delegate.readInt();
            Tuple3<TypeSerializer,TypeSerializer,TypeSerializer> typeSerializers =
                    typeSerializerGetter.apply(stateName);

            TypeSerializer<K> keySerializer = typeSerializers.f0.duplicate();
            TypeSerializer<N> namespaceSerializer = typeSerializers.f1.duplicate();
            TypeSerializer<S> valueSerializer = typeSerializers.f2.duplicate();

            InputStream inputStream = new ByteBufInputStream(delegate);
            DataInputView div = new DataInputViewStreamWrapper(inputStream);

            for(int i = 0; i < numberOfStateMaps; i++) {
                StateMap stateMap = stateMapGetter.apply(stateName);
                int keyGroupIndex = div.readInt();
                int numEntries = div.readInt();
                for (int j = 0; j < numEntries; j++) {
                    N namespace = namespaceSerializer.deserialize(div);
                    K key = keySerializer.deserialize(div);
                    S value = valueSerializer.deserialize(div);
                    stateMap.put(key, namespace, value);
                }
                stateMaps.put(keyGroupIndex, stateMap);
            }

            delegate.release(); // release the buffer after reading as receiver
            return new StateBuffer(stateName, stateMaps, bufferID);
        }
    }

}
