package org.apache.flink.runtime.scale.state.migrate;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.api.serialization.NonSpanningWrapper;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

import static java.lang.Math.min;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Aim to handle deserialization of buffers with partial record.
 * Since we need to let confirm barrier be kind of semi-unaligned
 * to bypass the in-flight data in output buffer.
 * Further, we should re-distribute the buffers according to the new routing table.
 */
public class RepartitionBuffersWithPartialRecord {
    protected static final Logger LOG = LoggerFactory.getLogger(RepartitionBuffersWithPartialRecord.class);

    private final Stack<BufferConsumerWithPartialRecordLength> buffers;

    private final NonSpanningWrapper nonSpanningWrapper;
    @Nullable private Buffer currentBuffer;

    private final SpanningHelper spanningHelper;

    public final static RepartitionBuffersWithPartialRecord EMPTY = new RepartitionBuffersWithPartialRecord();

    private RepartitionBuffersWithPartialRecord(){
        this(new Stack<>(), null);
    }

    public RepartitionBuffersWithPartialRecord(
            Stack<BufferConsumerWithPartialRecordLength> buffers, @Nullable MemorySegment partialRecordMemorySegment) {
        this.buffers = buffers;

        nonSpanningWrapper = new NonSpanningWrapper();
        spanningHelper = new SpanningHelper();


        if (partialRecordMemorySegment != null) {
            Preconditions.checkArgument(
                    !buffers.isEmpty(),
                    "Buffers should not be empty when partial record is not null");
            spanningHelper.transferFromPartialMemorySegment(partialRecordMemorySegment);
        }else{
            setNextBuffer();
        }
        LOG.info("Create RepartitionBuffersWithPartialRecord: {}", this);
    }

    @Override
    public String toString() {
        if (currentBuffer == null){
            return "RepartitionBuffersWithPartialRecord[no data]";
        }else{
            if (spanningHelper.recordLength != -1){
                return "RepartitionBuffersWithPartialRecord[" + (buffers.size()) + " buffers," +
                        "{" + (spanningHelper.accumulatedRecordBytes+spanningHelper.leftOverLimit-spanningHelper.leftOverStart) + "/"
                        + spanningHelper.recordLength + " bytes in spanningHelper}]";
            }else{
                return "RepartitionBuffersWithPartialRecord[" + (buffers.size()) + " buffers," +
                        "{" + spanningHelper.getNumGatheredBytes() + " bytes in spanningHelper}]";
            }

        }
    }


    public <T extends IOReadableWritable> DeserializationResult readNextRecord(T target)
            throws IOException {
        DeserializationResult result;
        if(nonSpanningWrapper.hasCompleteLength()) {
            int recordLength = nonSpanningWrapper.readInt();
            if (nonSpanningWrapper.canReadRecord(recordLength)){
                result =  nonSpanningWrapper.readInto(target);
            }else{
                // current buffer is not enough to read the record
                spanningHelper.transferFromNonSpanningWrapper(nonSpanningWrapper, recordLength);
                result =  RecordDeserializer.DeserializationResult.PARTIAL_RECORD;
            }
        }else if(nonSpanningWrapper.hasRemaining()) {
            // <= 4 bytes
            nonSpanningWrapper.transferTo(spanningHelper.lengthBuffer);
            result =  RecordDeserializer.DeserializationResult.PARTIAL_RECORD;
        }else if(spanningHelper.hasFullRecord()){
            target.read(spanningHelper.serializationReadBuffer);
            spanningHelper.transferLeftOverTo(nonSpanningWrapper);
            result =  nonSpanningWrapper.hasRemaining()
                    ? RecordDeserializer.DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER
                    : RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER;
        }else if(buffers.isEmpty()){
            if (spanningHelper.getNumGatheredBytes() > 0) {
                LOG.error("spanningHelper remaining {} bytes, and current recordLength is {}",
                        nonSpanningWrapper.remaining(), spanningHelper.recordLength);
                throw new IllegalStateException("There is no more data to read, but there is partial record");
            }
            checkState(spanningHelper.getNumGatheredBytes() == 0,
                    "There should be no partial record when there is no buffer," +
                            " but spanned helper has " + spanningHelper.accumulatedRecordBytes +
                            " accumulated bytes, and record length is " + spanningHelper.recordLength);
            return null; // no more data
        } else{
            result =  RecordDeserializer.DeserializationResult.PARTIAL_RECORD;
        }

        if(result.isBufferConsumed() && currentBuffer != null){
            currentBuffer.recycleBuffer();;
            currentBuffer = null;
        }
        return result;
    }

    public boolean setNextBuffer() {
        if (buffers.isEmpty()) {
            return false;
        }
        final BufferConsumer bufferConsumer = buffers.pop().getBufferConsumer();
        final Buffer buffer = bufferConsumer.build();
        checkState(bufferConsumer.isFinished(), "BufferConsumer must be finished");
        bufferConsumer.close();
        if (currentBuffer != null) {
            currentBuffer.recycleBuffer();
        }
        currentBuffer = buffer;
        checkNotNull(currentBuffer, "Buffer must not be null");
        if (!buffer.isBuffer()) {
            LOG.warn("BufferConsumer did not create a buffer: {}, {}", buffer.getClass().getName(), buffer.getDataType());
            throw new IllegalStateException("BufferConsumer did not create a buffer");
        }
        int offset = buffer.getMemorySegmentOffset();
        int numBytes = buffer.getSize();
        MemorySegment memorySegment = buffer.getMemorySegment();
        if (memorySegment == null) {
            LOG.error("buffer instance: {} with null memory segment", buffer.getClass().getName());
            throw new IllegalStateException("MemorySegment is null");
        }

        if(spanningHelper.getNumGatheredBytes() > 0) {
            spanningHelper.addNextChunkFromMemorySegment(memorySegment, offset, numBytes);
        }else{
            nonSpanningWrapper.initializeFromMemorySegment(memorySegment, offset, offset + numBytes);
        }
        LOG.info("Successfully set next buffer, remaining buffers: {}", buffers.size());
        return true;
    }

    private static class SpanningHelper{

        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        int recordLength = -1;

        private byte[] partialRecord = null;
        int accumulatedRecordBytes = 0;

        private MemorySegment leftOverData;
        private int leftOverStart;
        private int leftOverLimit;

        final DataInputDeserializer serializationReadBuffer = new DataInputDeserializer();

        public void transferFromNonSpanningWrapper(NonSpanningWrapper partial, int nextRecordLength) {
            lengthBuffer.clear();
            recordLength = nextRecordLength;
            partialRecord = new byte[recordLength];
            accumulatedRecordBytes = partial.copyContentTo(partialRecord);
            partial.clear();
        }

        public void transferFromPartialMemorySegment(MemorySegment partialSegment){
            // read length
            lengthBuffer.clear();
            int bytesToRead = min(lengthBuffer.remaining(), partialSegment.size());
            partialSegment.get(0, lengthBuffer, bytesToRead);
            if(!lengthBuffer.hasRemaining()) {
                recordLength = lengthBuffer.getInt(0);
                LOG.info("Transfer from partial memory segment, read record length: {}", recordLength);
                lengthBuffer.clear();
                // copy the rest of the record to partialRecord
                partialRecord = new byte[recordLength];
                int toCopy = min(partialSegment.size() - Integer.BYTES, recordLength);
                partialSegment.get(Integer.BYTES, partialRecord, 0, toCopy);
                accumulatedRecordBytes = toCopy;
                if (accumulatedRecordBytes == recordLength) {
                    serializationReadBuffer.setBuffer(partialRecord, 0, recordLength);
                }
                if (partialSegment.size() > Integer.BYTES + toCopy) {
                    leftOverData = partialSegment;
                    leftOverStart = Integer.BYTES + toCopy;
                    leftOverLimit = partialSegment.size();
                }
            }
        }

        public boolean hasFullRecord() {
            return recordLength > 0 && partialRecord != null && accumulatedRecordBytes >= recordLength;
        }

        public void addNextChunkFromMemorySegment(MemorySegment segment, int offset,int numBytes){
            LOG.info("addNextChunkFromMemorySegment, offset: {}, numBytes: {}, lengthBuffer position: {}",
                    offset, numBytes, lengthBuffer.position());
            int limit = offset + numBytes;
            int numBytesRead = 0;
            if (lengthBuffer.position() > 0){
                int bytesToRead = min(lengthBuffer.remaining(), numBytes);
                segment.get(offset, lengthBuffer, bytesToRead);
                if(!lengthBuffer.hasRemaining()) {
                    recordLength = lengthBuffer.getInt(0);
                    lengthBuffer.clear();
                    if (partialRecord == null) {
                        partialRecord = new byte[recordLength];
                    }
                    LOG.info("Read record length in addNextChunkFromMemorySegment: {}", recordLength);
                }
                numBytesRead = bytesToRead;
            }else{
                LOG.info("No lengthBuffer remaining, current recordLength: {}", recordLength);
            }

            offset += numBytesRead;
            numBytes -= numBytesRead;
            if (numBytes == 0) {
                return;
            }
            int toCopy = min(recordLength - accumulatedRecordBytes, numBytes);
            if(toCopy > 0) {
                segment.get(offset, partialRecord, accumulatedRecordBytes, toCopy);
                accumulatedRecordBytes += toCopy;
                if(hasFullRecord()){
                    serializationReadBuffer.setBuffer(partialRecord, 0, recordLength);
                }
            }
            if(numBytes > toCopy){
                leftOverData = segment;
                leftOverStart = offset + toCopy;
                leftOverLimit = limit;
            }
        }

        public void transferLeftOverTo(NonSpanningWrapper nonSpanningWrapper) {
            nonSpanningWrapper.clear();
            if(leftOverData != null){
                nonSpanningWrapper.initializeFromMemorySegment(leftOverData, leftOverStart, leftOverLimit);
            }
            LOG.info("transferLeftOverTo, leftOverData: {}, leftOverStart: {}, leftOverLimit: {}",
                    leftOverData, leftOverStart, leftOverLimit);
            clear();
        }

        public void clear(){
            recordLength = -1;
            partialRecord = null;
            accumulatedRecordBytes = 0;
            leftOverData = null;
            leftOverStart = 0;
            leftOverLimit = 0;
            lengthBuffer.clear();
        }

        public int getNumGatheredBytes() {
            return accumulatedRecordBytes +
                    (recordLength >= 0 ? Integer.BYTES : lengthBuffer.position());
        }
    }

}
