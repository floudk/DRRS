package org.apache.flink.runtime.scale.io;

import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for listening to the incoming messages from {@link ScaleCommManager}.
 * It is responsible for handling the buffer or event messages and take appropriate actions.
 * Notice, this class has no connection with the serialization or deserialization,
 * since it only listens from the manager, instead of the netty channel.
 */
public class ScaleCommListener {

    // These two consumers may be null at non-scaling phase as placeholder.
    // However, they should be set before any message is received.
    private BiConsumer<ScaleBuffer, Integer> bufferConsumer;
    private Consumer<ScaleEvent> eventConsumer;

    public void setConsumers(
            BiConsumer<ScaleBuffer, Integer>  bufferConsumer, Consumer<ScaleEvent> eventConsumer) {
        this.bufferConsumer = bufferConsumer;
        this.eventConsumer = eventConsumer;
    }

    public void onBuffer(ScaleBuffer buffer, int fromTaskIndex) {
        checkNotNull(bufferConsumer, "Buffer consumer is not set");
        bufferConsumer.accept(buffer, fromTaskIndex);
    }

    public void onEvent(ScaleEvent event) {
        checkNotNull(eventConsumer, "Event consumer is not set");
        eventConsumer.accept(event);
    }
}
