package org.apache.flink.runtime.scale.io.message;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public abstract class ScaleEvent {

    public final int eventSenderIndex; // the index of the subtask that sends the event

    public ScaleEvent(int eventSenderIndex) {
        this.eventSenderIndex = eventSenderIndex;
    }
    // Return the size of the event, used for serialization
    public abstract int getSize();

    public abstract void writeTo(ByteBuf buf);

    public static ScaleEvent fromByteBuf(ByteBuf in) {
        byte id = in.readByte();
        switch (id) {
            case RequestStates.ID:
                return RequestStates.readFrom(in);
            case AcknowledgeStates.ID:
                return AcknowledgeStates.readFrom(in);
            default:
                throw new IllegalArgumentException("Unknown event ID: " + id);
        }
    }


    public static class RequestStates extends ScaleEvent {

        static final byte ID = 0;
        public List<Integer> requestedStates;
        public final int subscaleID;

        public RequestStates(int subtaskIndex, List<Integer> keyGroups, int subscaleID) {
            super(subtaskIndex);
            this.requestedStates = keyGroups;
            this.subscaleID = subscaleID;
        }

        /**
         * eventSenderIndex: 4 bytes
         * ID: 1 byte
         * requestedStates size: 4 bytes
         * requestedStates: 4 * requestedStates.size() bytes
         *
         * @return
         */
        @Override
        public int getSize() {
            return 1 + 4 + 4 + 4 * requestedStates.size() + 4;
        }

        @Override
        public void writeTo(ByteBuf buf) {
            buf.writeByte(ID);
            buf.writeInt(eventSenderIndex);
            buf.writeInt(requestedStates.size());
            for (int keyGroup : requestedStates) {
                buf.writeInt(keyGroup);
            }
            buf.writeInt(subscaleID);
        }

        public static RequestStates readFrom(ByteBuf buf) {
            int subtaskIndex = buf.readInt();
            int size = buf.readInt();
            List<Integer> keyGroups = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                keyGroups.add(buf.readInt());
            }
            int subscaleID = buf.readInt();
            return new RequestStates(subtaskIndex, keyGroups, subscaleID);
        }
        @Override
        public String toString() {
            return "RequestStates-from-" + eventSenderIndex + " : " + requestedStates;
        }
    }

    public static class AcknowledgeStates extends ScaleEvent {

        static final byte ID = 1;
        public int keyGroupIndex;

        public AcknowledgeStates(int eventSenderIndex, int keyGroupIndex) {
            super(eventSenderIndex);
            this.keyGroupIndex = keyGroupIndex;
        }

        @Override
        public int getSize() {
            // eventSenderIndex: 4 bytes
            // ID: 1 byte
            // stateBufferID: 4 bytes
            return 1 + 4 + 4;
        }

        @Override
        public void writeTo(ByteBuf buf) {
            buf.writeByte(ID);
            buf.writeInt(eventSenderIndex);
            buf.writeInt(keyGroupIndex);
        }

        public static AcknowledgeStates readFrom(ByteBuf buf) {
            int subtaskIndex = buf.readInt();
            int stateBufferID = buf.readInt();
            return new AcknowledgeStates(subtaskIndex, stateBufferID);
        }
    }

}


