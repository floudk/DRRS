package org.apache.flink.runtime.scale.io.network;

import org.apache.flink.runtime.scale.io.ScaleCommManager;
import org.apache.flink.runtime.scale.io.TaskScaleID;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@ChannelHandler.Sharable
public class TaskScaleHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(TaskScaleHandler.class);
    private ScaleCommManager commManager;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        checkNotNull(commManager, "commManager is not set");
        // LOG.info("Received message from {}", ctx.channel().remoteAddress());
        // Handle the message
        try{
            ScaleNettyMessage scaleNettyMessage = (ScaleNettyMessage) msg;

            final TaskScaleID senderID = new TaskScaleID(
                    scaleNettyMessage.getJobVertexID(), scaleNettyMessage.getSender());
            commManager.registerChannel(senderID, ctx.channel());

            Class<?> msgClazz = scaleNettyMessage.getClass();
            if (msgClazz == ScaleNettyMessage.ScaleEventMessage.class){
                commManager.onRemoteEventMessage(
                        (ScaleNettyMessage.ScaleEventMessage) scaleNettyMessage);
            }else if (msgClazz == ScaleNettyMessage.ScaleBufferMessage.class){
                commManager.onRemoteBufferMessage(
                        (ScaleNettyMessage.ScaleBufferMessage) scaleNettyMessage);
            }else{
                LOG.error("Unknown message type: {}", msgClazz);
                throw new IllegalArgumentException("Unknown message type: " + msgClazz);
            }

        }catch (Exception e){
            LOG.error("Error while handling message", e);
            throw e;
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    public void setProcessor(ScaleCommManager commManager) {
        this.commManager = commManager;
    }
}
