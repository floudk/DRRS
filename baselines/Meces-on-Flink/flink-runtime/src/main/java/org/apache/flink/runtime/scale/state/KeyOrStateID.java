package org.apache.flink.runtime.scale.state;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;

public class KeyOrStateID {
    int keyGroupIndex;
    HierarchicalStateID stateID;
    public KeyOrStateID(int keyGroupIndex) {
        this.keyGroupIndex = keyGroupIndex;
        this.stateID = null;
    }
    public KeyOrStateID(HierarchicalStateID stateID) {
        this.keyGroupIndex = -1;
        this.stateID = stateID;
    }

    public boolean isKey() {
        return keyGroupIndex != -1;
    }
    public HierarchicalStateID getStateID() {
        return stateID;
    }

    public int getKey(){
        return isKey()? keyGroupIndex : stateID.keyGroupIndex;
    }

    @Override
    public String toString() {
        return isKey() ? keyGroupIndex + "" : stateID.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()){
            return false;

        }
        KeyOrStateID other = (KeyOrStateID) obj;

        if (keyGroupIndex != other.keyGroupIndex) {
            return false;
        }

        if (stateID == null && other.stateID == null) {
            return true;
        }
        if (stateID != null && other.stateID != null) {
            return stateID.equals(other.stateID);
        }

        return false;
    }

    @Override
    public int hashCode() {
        // stateID is null if isKey is true
        return isKey() ? keyGroupIndex : stateID.hashCode();
    }


    public void writeTo(DataOutputViewStreamWrapper dataOutput) throws IOException {
        if (isKey()) {
            dataOutput.writeBoolean(true);
            dataOutput.writeInt(keyGroupIndex);
        } else {
            dataOutput.writeBoolean(false);
            stateID.writeTo(dataOutput);
        }
    }
    public static KeyOrStateID readFrom(DataInputView div) throws IOException {
        boolean isKey = div.readBoolean();
        if (isKey) {
            return new KeyOrStateID(div.readInt());
        } else {
            return new KeyOrStateID(HierarchicalStateID.readFrom(div));
        }
    }

    public int getSize() {
        // 4 bytes for a boolean and 4 bytes key, 8 for stateID (2 ints)
        return 1 + (isKey() ? 4 : 8);
    }

    public void writeTo(ByteBuf buf) {
        if (isKey()) {
            buf.writeBoolean(true);
            buf.writeInt(keyGroupIndex);
        } else {
            buf.writeBoolean(false);
            stateID.writeTo(buf);
        }
    }
    public static KeyOrStateID readFrom(ByteBuf buf) {
        boolean isKey = buf.readBoolean();
        if (isKey) {
            return new KeyOrStateID(buf.readInt());
        } else {
            return new KeyOrStateID(HierarchicalStateID.readFrom(buf));
        }
    }

}
