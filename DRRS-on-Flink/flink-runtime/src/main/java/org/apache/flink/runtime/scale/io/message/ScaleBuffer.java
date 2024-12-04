package org.apache.flink.runtime.scale.io.message;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.runtime.scale.io.network.ScaleNettyMessage.ScaleNettyMessage_HEADER_LENGTH;


public abstract class ScaleBuffer {
    protected static final Logger LOG = LoggerFactory.getLogger(ScaleBuffer.class);

    public static ScaleBuffer fromByteBuf(ByteBuf in) {
        byte id = in.readByte();
        switch (id) {
            case RecordBuffer.BTYPE:
                return RecordBuffer.fromByteBuf(in);
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

    public static class RecordBuffer extends ScaleBuffer {
        static final byte BTYPE = 0;

        @Nullable public final RerouteCache rerouteCache;

        public final boolean withConfirmBarrier;
        @Nullable public final InputChannelInfo inputChannelInfo;
        @Nullable public final Set<Integer> confirmedKeys;

        public final int subscaleID;
        public final int sequenceNumber;

        public RecordBuffer(
                int subscaleID,
                int sequenceNumber,
                @Nullable RerouteCache rerouteCache,
                boolean withConfirmBarrier,
                @Nullable InputChannelInfo inputChannelInfo,
                @Nullable Set<Integer> confirmedKeys) {

            this.subscaleID = subscaleID;
            this.sequenceNumber = sequenceNumber;

            this.rerouteCache = rerouteCache;
            this.withConfirmBarrier = withConfirmBarrier;
            this.inputChannelInfo = inputChannelInfo;
            this.confirmedKeys = confirmedKeys;
        }

        @Override
        public int getSize() {
            return getSizeWithOutRerouteCache()
                    + (rerouteCache == null ? 0 : rerouteCache.getBuffer().readableBytes());
        }

        private int getSizeWithOutRerouteCache() {
            int size = 1; // buffer type
            size += 8; // subscaleID + sequenceNumber
            size += 1; // withConfirmBarrier
            if (withConfirmBarrier) {
                size += inputChannelInfo.getSerializedSize();
                size += 4 + confirmedKeys.size() * 4;
            }
            return size;
        }

        @Override
        public ByteBuf write(
                ByteBufAllocator allocator, byte ID, JobVertexID jobVertexID, int sender, int receiver) {
            CompositeByteBuf compositeByteBuf = allocator.compositeBuffer();
            ByteBuf header = NettyMessage.allocateBuffer(
                    allocator,
                    ID,
                    ScaleNettyMessage_HEADER_LENGTH + getSizeWithOutRerouteCache(),
                    getSize() - getSizeWithOutRerouteCache(),
                    false);
            jobVertexID.writeTo(header);
            header.writeInt(sender);
            header.writeInt(receiver);

            header.writeByte(BTYPE); // buffer type
            header.writeInt(subscaleID);
            header.writeInt(sequenceNumber);
            header.writeBoolean(withConfirmBarrier);
            if (withConfirmBarrier) {
                inputChannelInfo.writeTo(header);
                header.writeInt(confirmedKeys.size());
                for (int key : confirmedKeys) {
                    header.writeInt(key);
                }
            }

            compositeByteBuf.addComponent(true, header);

            if (rerouteCache != null) {
                compositeByteBuf.addComponent(true, rerouteCache.getBuffer().slice());
                LOG.info("RerouteBuffer created with {} Bytes", rerouteCache.getBuffer().readableBytes());
            }
            return compositeByteBuf;
        }

        public static RecordBuffer fromByteBuf(ByteBuf in){
            LOG.info("RecordBuffer created with {} Bytes", in.readableBytes());
            int subscaleID = in.readInt();
            int sequenceNumber = in.readInt();

            boolean withConfirmBarrier = in.readBoolean();
            InputChannelInfo inputChannelInfo = null;
            Set<Integer> confirmedKeys = null;
            if (withConfirmBarrier) {
                confirmedKeys = new HashSet<>();
                inputChannelInfo = InputChannelInfo.fromByteBuf(in);
                int numConfirmedKeys = in.readInt();
                for (int i = 0; i < numConfirmedKeys; i++) {
                    confirmedKeys.add(in.readInt());
                }
            }

            RerouteCache rerouteCache = null;
            if (in.isReadable()) {
                in.retain(); // retain the buffer to prevent it from being released
                rerouteCache = new RerouteCache.RemoteDelegate(in.slice());
            }

            return new RecordBuffer(
                    subscaleID,
                    sequenceNumber,
                    rerouteCache,
                    withConfirmBarrier,
                    inputChannelInfo,
                    confirmedKeys);
        }
    }


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
                    ScaleNettyMessage_HEADER_LENGTH + 1, // 4 for bufferID, 1 for buffer type
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
            return delegate.readableBytes() + 4 + 1;
        }

        public static StateBufferDelegate fromByteBuf(ByteBuf in) {
            // retain the buffer to prevent it from being released
            return new StateBufferDelegate(in.retain());
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
            return new StateBuffer(stateName, stateMaps);
        }
    }

}
