package org.apache.flink.runtime.scale.state;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.flink.runtime.scale.ScaleConfig;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.Serializable;

public class HierarchicalStateID implements Serializable{
    private static final long serialVersionUID = 1L;

    public final int keyGroupIndex;
    public final int binIndex;

    public HierarchicalStateID(int keyGroupIndex, int binIndex) {
        this.keyGroupIndex = keyGroupIndex;
        this.binIndex = binIndex;
    }


    @Override
    public String toString() {
        return keyGroupIndex + ":" + binIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HierarchicalStateID) {
            HierarchicalStateID other = (HierarchicalStateID) obj;
            return keyGroupIndex == other.keyGroupIndex && binIndex == other.binIndex;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // binIndex is in [0, ScaleConfig.Instance.HIERARCHICAL_BIN_NUM)
        return ScaleConfig.Instance.HIERARCHICAL_BIN_NUM * keyGroupIndex + binIndex;
    }

    public void writeTo(ByteBuf buf) {
        buf.writeInt(keyGroupIndex);
        buf.writeInt(binIndex);
    }

    public void writeTo(DataOutputViewStreamWrapper dataOutput) throws IOException {
        dataOutput.writeInt(keyGroupIndex);
        dataOutput.writeInt(binIndex);
    }

    public static HierarchicalStateID readFrom(ByteBuf buf) {
        return new HierarchicalStateID(buf.readInt(), buf.readInt());
    }

    public static HierarchicalStateID readFrom(DataInputView div) throws IOException {
        return new HierarchicalStateID(div.readInt(), div.readInt());
    }

}
