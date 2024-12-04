package org.apache.flink.runtime.scale.io.message.barrier;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public abstract class ScaleBarrier extends RuntimeEvent implements Serializable {
    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }


    public static BufferConsumer toBufferConsumer(AbstractEvent event, Buffer.DataType type)
            throws IOException {
        final ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

        MemorySegment data = MemorySegmentFactory.wrap(serializedEvent.array());

        return new BufferConsumer(
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, type),
                data.size());
    }

    public static class CompleteBarrier extends ScaleBarrier {
        private static final long serialVersionUID = 1L;
        @Override
        public String toString() {
            return "CompleteBarrier";
        }
    }


}
