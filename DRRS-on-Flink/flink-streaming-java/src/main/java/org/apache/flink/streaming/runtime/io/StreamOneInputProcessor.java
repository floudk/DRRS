/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.scale.util.ThrowingBiConsumer;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public final class StreamOneInputProcessor<IN> implements StreamInputProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamOneInputProcessor.class);

    private StreamTaskInput<IN> input;
    private DataOutput<IN> output;

    private final BoundedMultiInput endOfInputAware;

    public StreamOneInputProcessor(
            StreamTaskInput<IN> input, DataOutput<IN> output, BoundedMultiInput endOfInputAware) {

        this.input = checkNotNull(input);
        this.output = checkNotNull(output);
        this.endOfInputAware = checkNotNull(endOfInputAware);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return input.getAvailableFuture();
    }

    @Override
    public DataInputStatus processInput() throws Exception {
        DataInputStatus status = input.emitNext(output);

        if (status == DataInputStatus.END_OF_DATA) {
            endOfInputAware.endInput(input.getInputIndex() + 1);
            output = new FinishedDataOutput<>();
        } else if (status == DataInputStatus.END_OF_RECOVERY) {
            if (input instanceof RecoverableStreamTaskInput) {
                input = ((RecoverableStreamTaskInput<IN>) input).finishRecovery();
            }
            return DataInputStatus.MORE_AVAILABLE;
        }

        return status;
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        return input.prepareSnapshot(channelStateWriter, checkpointId);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
    // ------------------------------------------------------------------------
    // Scale Utils
    // ------------------------------------------------------------------------

    @Override
    public void triggerScale() throws Exception {
        output.initialScaling();
    }

    public void expandOneInputProcessor(
            int newChannelNum, List<InputChannelInfo> addedChannelInfos, IOManager ioManager) {
        input.expandRecordDeserializers(newChannelNum, addedChannelInfos, ioManager);
    }

    public ThrowingBiConsumer<StreamRecord, InputChannelInfo, Exception> getRecordProcessorInScaling() {
        return output::emitRecordDuringScaling;
    }

}
