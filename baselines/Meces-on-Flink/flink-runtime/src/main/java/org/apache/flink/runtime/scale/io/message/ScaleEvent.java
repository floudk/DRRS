package org.apache.flink.runtime.scale.io.message;

import org.apache.flink.runtime.scale.state.HierarchicalStateID;
import org.apache.flink.runtime.scale.state.KeyOrStateID;

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
            case StatesRequest.ID:
                return StatesRequest.readFrom(in);
            case FetchRequest.ID:
                return FetchRequest.readFrom(in);
            case Acknowledge.ID:
                return Acknowledge.readFrom(in);
            case NoBinAck.ID:
                return NoBinAck.readFrom(in);
            case AlignedAck.ID:
                return AlignedAck.readFrom(in);
            default:
                throw new IllegalArgumentException("Unknown event ID: " + id);
        }
    }


    public static class StatesRequest extends ScaleEvent {

        static final byte ID = 0;
        public List<Integer> requestedStates;

        public StatesRequest(int subtaskIndex, List<Integer> keyGroups) {
            super(subtaskIndex);
            this.requestedStates = keyGroups;
        }

        /**
         * eventSenderIndex: 4 bytes
         * ID: 1 byte
         * requestedStates size: 4 bytes
         * requestedStates: 4 * requestedStates.size() bytes
         */
        @Override
        public int getSize() {
            return  1 + 4 + 4 + 4 * requestedStates.size();
        }

        @Override
        public void writeTo(ByteBuf buf) {
            buf.writeByte(ID);
            buf.writeInt(eventSenderIndex);
            buf.writeInt(requestedStates.size());
            for (int keyGroup : requestedStates) {
                buf.writeInt(keyGroup);
            }
        }

        public static StatesRequest readFrom(ByteBuf buf) {
            int subtaskIndex = buf.readInt();
            int size = buf.readInt();
            List<Integer> keyGroups = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                keyGroups.add(buf.readInt());
            }
            return new StatesRequest(subtaskIndex, keyGroups);
        }
        @Override
        public String toString() {
            return "RequestStates-from-" + eventSenderIndex + " : " + requestedStates;
        }
    }

    public static class FetchRequest extends ScaleEvent{
        static final byte ID = 1;
        public HierarchicalStateID fetchedStateID;

        public FetchRequest(int eventSenderIndex, HierarchicalStateID fetchedStateID){
            super(eventSenderIndex);
            this.fetchedStateID = fetchedStateID;
        }

        @Override
        public int getSize(){
            // eventSenderIndex: 4 bytes
            // ID: 1 byte
            // fetchedStateID: 2 * 4 bytes
            return 1 + 4 + 8;
        }

        @Override
        public void writeTo(ByteBuf buf){
            buf.writeByte(ID);
            buf.writeInt(eventSenderIndex);
            fetchedStateID.writeTo(buf);
        }

        public static FetchRequest readFrom(ByteBuf buf){
            return new FetchRequest(buf.readInt(), HierarchicalStateID.readFrom(buf));
        }
    }

    public static class Acknowledge extends ScaleEvent {
        static final byte ID = 2;
        public KeyOrStateID ackedID;

        public Acknowledge(int eventSenderIndex, KeyOrStateID id) {
            super(eventSenderIndex);
            this.ackedID = id;
        }

        @Override
        public int getSize() {
            // eventSenderIndex: 4 bytes
            // ID: 1 byte
            return 1 + 4 + ackedID.getSize();
        }

        @Override
        public void writeTo(ByteBuf buf) {
            buf.writeByte(ID);
            buf.writeInt(eventSenderIndex);
            ackedID.writeTo(buf);
        }

        public static Acknowledge readFrom(ByteBuf buf) {
            int subtaskIndex = buf.readInt();
            return new Acknowledge(subtaskIndex, KeyOrStateID.readFrom(buf));
        }
    }

    public static class NoBinAck extends ScaleEvent{
        static final byte ID = 3;
        public HierarchicalStateID noBinAckID;
        public NoBinAck(int eventSenderIndex, HierarchicalStateID id){
            super(eventSenderIndex);
            this.noBinAckID = id;
        }

        @Override
        public int getSize(){
            // eventSenderIndex: 4 bytes
            // ID: 1 byte
            return 1 + 4 + 8;
        }

        @Override
        public void writeTo(ByteBuf buf){
            buf.writeByte(ID);
            buf.writeInt(eventSenderIndex);
            noBinAckID.writeTo(buf);
        }

        public static NoBinAck readFrom(ByteBuf buf){
            int subtaskIndex = buf.readInt();
            return new NoBinAck(subtaskIndex, HierarchicalStateID.readFrom(buf));
        }
    }

    public static class AlignedAck extends ScaleEvent{
        static final byte ID =4;
        public AlignedAck(int eventSenderIndex){
            super(eventSenderIndex);
        }
        @Override
        public int getSize(){
            // eventSenderIndex: 4 bytes
            // ID: 1 byte
            return 1 + 4;
        }
        @Override
        public void writeTo(ByteBuf buf){
            buf.writeByte(ID);
            buf.writeInt(eventSenderIndex);
        }

        public static AlignedAck readFrom(ByteBuf buf){
            int subtaskIndex = buf.readInt();
            return new AlignedAck(subtaskIndex);
        }
    }

}


