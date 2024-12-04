package org.apache.flink.streaming.runtime.scale.migrate.strategy.reroute;

import org.apache.flink.runtime.scale.io.message.RerouteCache;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkState;

public class LocalRerouteCache extends RerouteCache<StreamRecord> {
    public final Queue<StreamRecord> buffer;

    public LocalRerouteCache(
            int targetTaskIndex,
            Runnable timeExpiredHandler,
            long bufferTimeout,
            long bufferCapacity) {
        super(targetTaskIndex,timeExpiredHandler, bufferTimeout, bufferCapacity);
        this.buffer = new ArrayDeque<>((int) bufferCapacity);
    }

    @Override
    public void initWithPreviousOverflow(StreamRecord record, RerouteCache oldOne) {
        checkState(buffer.isEmpty(), "buffer must be empty");
        buffer.add(record);
        updateLastUpdateTime();
    }


    @Override
    public boolean addRecord(StreamRecord record) {
        if (buffer.size() >= bufferCapacity) {
            return false;
        }
        buffer.add(record);
        updateLastUpdateTime();
        return true;
    }

}
