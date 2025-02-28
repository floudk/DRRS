package org.apache.flink.runtime.scale.io;

import java.io.Serializable;

public class SubscaleTriggerInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    public final Integer newPartitioningPos;
    public final Long stateSize;
    public SubscaleTriggerInfo(int newPartitioningPos, long stateSize) {
        this.newPartitioningPos = newPartitioningPos;
        this.stateSize = stateSize;
    }

    @Override
    public String toString() {
        return "[newPos: " + newPartitioningPos + ", stateSize: " + stateSize + "]";
    }
}
