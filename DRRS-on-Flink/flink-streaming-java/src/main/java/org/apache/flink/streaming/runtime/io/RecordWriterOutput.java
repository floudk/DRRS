/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.WatermarkGaugeExposingOutput;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation of {@link Output} that sends data using a {@link RecordWriter}. */
@Internal
public class RecordWriterOutput<OUT> implements WatermarkGaugeExposingOutput<StreamRecord<OUT>> {
    private static Logger LOG = LoggerFactory.getLogger(RecordWriterOutput.class);

    private RecordWriter<SerializationDelegate<StreamElement>> recordWriter;

    private SerializationDelegate<StreamElement> serializationDelegate;

    private final boolean supportsUnalignedCheckpoints;

    private final OutputTag outputTag;

    private final WatermarkGauge watermarkGauge = new WatermarkGauge();

    private WatermarkStatus announcedStatus = WatermarkStatus.ACTIVE;

    @SuppressWarnings("unchecked")
    public RecordWriterOutput(
            RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
            TypeSerializer<OUT> outSerializer,
            OutputTag outputTag,
            boolean supportsUnalignedCheckpoints) {

        checkNotNull(recordWriter);
        this.outputTag = outputTag;
        // generic hack: cast the writer to generic Object type so we can use it
        // with multiplexed records and watermarks
        this.recordWriter =
                (RecordWriter<SerializationDelegate<StreamElement>>) (RecordWriter<?>) recordWriter;

        TypeSerializer<StreamElement> outRecordSerializer =
                new StreamElementSerializer<>(outSerializer);

        if (outSerializer != null) {
            serializationDelegate = new SerializationDelegate<>(outRecordSerializer);
        }

        this.supportsUnalignedCheckpoints = supportsUnalignedCheckpoints;
    }

    @Override
    public void collect(StreamRecord<OUT> record) {
        if (this.outputTag != null) {
            // we are not responsible for emitting to the main output.
            return;
        }
        if(Thread.currentThread().getName().contains("WindowedSum")){
            LOG.info("RecordWriterOutput.collect: {}", record.getValue());
        }
        pushToRecordWriter(record);
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        if (OutputTag.isResponsibleFor(this.outputTag, outputTag)) {
            pushToRecordWriter(record);
        }
    }

    private <X> void pushToRecordWriter(StreamRecord<X> record) {
        serializationDelegate.setInstance(record);

        try {
            recordWriter.emit(serializationDelegate);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public void emitWatermark(Watermark mark) {
        if (announcedStatus.isIdle()) {
            return;
        }

        watermarkGauge.setCurrentWatermark(mark.getTimestamp());
        serializationDelegate.setInstance(mark);

        try {
            recordWriter.broadcastEmit(serializationDelegate);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        if (!announcedStatus.equals(watermarkStatus)) {
            announcedStatus = watermarkStatus;
            serializationDelegate.setInstance(watermarkStatus);
            try {
                recordWriter.broadcastEmit(serializationDelegate);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        serializationDelegate.setInstance(latencyMarker);

        try {
            recordWriter.randomEmit(serializationDelegate);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        if (isPriorityEvent
                && event instanceof CheckpointBarrier
                && !supportsUnalignedCheckpoints) {
            final CheckpointBarrier barrier = (CheckpointBarrier) event;
            event = barrier.withOptions(barrier.getCheckpointOptions().withUnalignedUnsupported());
            isPriorityEvent = false;
        }
        recordWriter.broadcastEvent(event, isPriorityEvent);
    }

    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        recordWriter.alignedBarrierTimeout(checkpointId);
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        recordWriter.abortCheckpoint(checkpointId, cause);
    }

    public void flush() throws IOException {
        recordWriter.flushAll();
    }

    @Override
    public void close() {
        recordWriter.close();
    }

    @Override
    public Gauge<Long> getWatermarkGauge() {
        return watermarkGauge;
    }
}
