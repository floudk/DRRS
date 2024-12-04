package org.apache.flink.streaming.runtime.scale.migrate.strategy.reroute;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.scale.io.message.RerouteCache;
import org.apache.flink.runtime.scale.util.ByteBufDataInputView;
import org.apache.flink.runtime.scale.util.ByteBufDataOutputView;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * By default, Flink has a 64MB/32KB = 2048 NetworkBuffers.
 * And reroute buffer will also be allocated from the network buffer pool.
 * <p>
 * There may be some problems:
 * 1. no enough network buffer to allocate for reroute buffer.
 * 2. 32KB is too big for reroute buffer,
 *    since the records to reroute are guaranteed to be smaller than input buffer size.
 * However, in the demo, we just ignore these problems with some assumptions.
 */
public class RemoteRerouteCache extends RerouteCache<StreamRecord> {

    private final ByteBuf buffer;
    private final TypeSerializer inputSerializer;

    ByteBuf pendingSerializedRecord = null;

    public RemoteRerouteCache(
            int targetTaskIndex,
            Runnable timeExpiredHandler,
            long bufferTimeout,
            long bufferCapacity,
            ByteBufAllocator allocator,
            TypeSerializer inputSerializer) {
        super(targetTaskIndex, timeExpiredHandler, bufferTimeout, bufferCapacity);
        this.buffer = allocator.buffer((int) bufferCapacity);
        this.inputSerializer = new StreamElementSerializer<>(checkNotNull(inputSerializer));
    }

    @Override
    public void initWithPreviousOverflow(StreamRecord record, RerouteCache oldOne) {
        ByteBuf overflowBuffer = ((RemoteRerouteCache) oldOne).pendingSerializedRecord;
        if (overflowBuffer == null) {
            return;
        }
        int recordLength = overflowBuffer.readableBytes();
        int totalLength = 4 + recordLength;
        checkState(buffer.writableBytes() >= totalLength,
                "Empty buffer should have enough space to write pending record");
        buffer.writeInt(recordLength);
        buffer.writeBytes(overflowBuffer);
        LOG.info("Write pending record with {} bytes to buffer {}", recordLength, targetTaskIndex);
        overflowBuffer.release();
    }

    // return false if the buffer is full after adding current record
    @Override
    public boolean addRecord(StreamRecord record) {
        ByteBuf tempBuffer = Unpooled.buffer();

        // serialize the record
        try {
            DataOutputView tempOutputView = new ByteBufDataOutputView(tempBuffer);
            inputSerializer.serialize(record, tempOutputView);
            int recordLength = tempBuffer.readableBytes();
            int totalLength = 4 + recordLength;

            if (buffer.writableBytes() < totalLength) {
                pendingSerializedRecord = tempBuffer;
                return false;
            }
            tempBuffer.readerIndex(0);
            buffer.writeInt(recordLength);
            buffer.writeBytes(tempBuffer);
        } catch (IOException e) {
            LOG.error("Failed to serialize record", e);
            throw new RuntimeException("Failed to serialize record", e);
        } finally {
            if (tempBuffer != pendingSerializedRecord) {
                tempBuffer.release();
            }
        }
        return buffer.isWritable();
    }

    @Override
    public ByteBuf getBuffer() {
        return buffer;
    }
    public int getBufferLength() {
        return buffer.readableBytes();
    }

    public static Queue<StreamRecord> deserialize(ByteBuf buffer, TypeSerializer is) throws IOException {
        TypeSerializer streamRecordSerializer = new StreamElementSerializer<>(is);
        Queue<StreamRecord> records = new ArrayDeque<>();
        ByteBufDataInputView inputView = new ByteBufDataInputView(buffer);

        // LOG the first 16 bytes of buffer, but do not change the readerIndex
        buffer.readerIndex(0);
        while (inputView.remaining() > 0) {
            int recordLength = inputView.readInt();
            LOG.info("Read record with {}/{} bytes from buffer", recordLength, inputView.remaining());
            checkState(inputView.remaining() >= recordLength,
                    "Invalid buffer: need " + recordLength + " bytes, but only " + inputView.remaining() + " bytes left");

            StreamRecord record = ((StreamElement) (streamRecordSerializer.deserialize(inputView))).asRecord();
            records.add(record);
        }
        checkState(inputView.remaining() == 0, "Invalid buffer");
        buffer.release();
        return records;
    }
}
