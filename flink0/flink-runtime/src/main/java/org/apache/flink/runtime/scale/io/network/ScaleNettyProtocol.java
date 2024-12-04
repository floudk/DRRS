package org.apache.flink.runtime.scale.io.network;

import org.apache.flink.runtime.scale.io.ScaleCommManager;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

public class ScaleNettyProtocol {
    private final ScaleNettyMessage.Encoder encoder = new ScaleNettyMessage.Encoder();

    /**
     * TODO: if this object is shared between multiple channels, we need to make sure that it is thread-safe.
     */
    private final TaskScaleHandler taskScaleHandler = new TaskScaleHandler();

    // Currently, we do not distinguish between server and client handlers.
    // This might change in the future if necessary.
    public ChannelHandler[] getHandlers(ScaleCommManager scaleCommManager){
        taskScaleHandler.setProcessor(scaleCommManager);
        return new ChannelHandler[] {
                encoder, // outbound
                new ScaleNettyMessage.Decoder(), // inbound
                taskScaleHandler, // inbound
        };
    }
}
