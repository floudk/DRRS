package org.apache.flink.runtime.scale.io.network;

import org.apache.flink.runtime.io.network.netty.NettyMessage;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

public abstract class ScaleNettyMessage extends NettyMessage {
    /**
     * Since all task in the same task manager share the same network connection for scaling,
     * we need to distinguish the message through JobVertexID and subtaskIndex to identify the target task.
     */
    public final static int ScaleNettyMessage_HEADER_LENGTH = JobVertexID.getByteBufLength() + 8;
    protected final JobVertexID jobVertexID;
    protected final int sender;
    protected final int receiver;

    protected ScaleNettyMessage(JobVertexID jobVertexID, int sender, int receiver) {
        this.jobVertexID = jobVertexID;
        this.sender = sender;
        this.receiver = receiver;
    }

    public JobVertexID getJobVertexID() {
        return jobVertexID;
    }
    public int getReceiver() {
        return receiver;
    }
    public int getSender() {
        return sender;
    }



    public static class ScaleEventMessage extends ScaleNettyMessage{
        private static final byte ID = 31;
        final ScaleEvent event;

        public ScaleEventMessage(JobVertexID jobVertexID, int sender, int receiver, ScaleEvent event) {
            super(jobVertexID, sender, receiver);
            this.event = event;
        }

        public ScaleEvent getEvent() {
            return event;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out,
                ChannelPromise promise,
                ByteBufAllocator allocator) throws IOException {
            ByteBuf buf = null;
            try{
                buf = NettyMessage.allocateBuffer(
                        allocator, ID, ScaleNettyMessage_HEADER_LENGTH + event.getSize());
                jobVertexID.writeTo(buf);
                buf.writeInt(sender);
                buf.writeInt(receiver);

                event.writeTo(buf);
                out.write(buf,promise);
            }catch (Throwable t){
                LOG.error("Error writing event message to channel.", t);
                handleException(buf,null,t);
            }
        }

        public static ScaleEventMessage readFrom(ByteBuf in) {
            JobVertexID jobVertexID = JobVertexID.fromByteBuf(in);
            int sender = in.readInt();
            int receiver = in.readInt();

            ScaleEvent event = ScaleEvent.fromByteBuf(in);
            return new ScaleEventMessage(jobVertexID, sender, receiver, event);
        }
    }

    public static class ScaleBufferMessage extends ScaleNettyMessage{
        private static final byte ID = 32;
        final ScaleBuffer buffer;
        public ScaleBufferMessage(JobVertexID jobVertexID, int sender, int receiver, ScaleBuffer buffer) {
            super(jobVertexID, sender, receiver);
            this.buffer = buffer;
        }

        public ScaleBuffer getBuffer() {
            return buffer;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out,
                ChannelPromise promise,
                ByteBufAllocator allocator) throws IOException {
            ByteBuf buf = null;
            try{
                buf = buffer.write(allocator, ID, jobVertexID, sender, receiver);
                out.write(buf,promise);
            }catch (Throwable t){
                handleException(buf,null,t);
            }
        }

        public static ScaleBufferMessage readFrom(ByteBuf in) {
            JobVertexID jobVertexID = JobVertexID.fromByteBuf(in);
            int sender = in.readInt();
            int receiver = in.readInt();

            ScaleBuffer buffer = ScaleBuffer.fromByteBuf(in);
            return new ScaleBufferMessage(jobVertexID, sender, receiver, buffer);
        }
    }


    @ChannelHandler.Sharable
    static class Encoder extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws IOException {
            checkArgument(msg instanceof NettyMessage);
            ((NettyMessage) msg).write(ctx, promise, ctx.alloc());
        }
    }

    static class Decoder extends LengthFieldBasedFrameDecoder {
        Decoder() {
            super(Integer.MAX_VALUE, 0, 4, -4, 4);
        }
        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf msg = (ByteBuf) super.decode(ctx, in);
            if (msg == null) {
                return null;
            }
            try{
                int magicNumber = msg.readInt();
                if (magicNumber != MAGIC_NUMBER) {
                    throw new IllegalStateException(
                            "Network stream corrupted: received incorrect magic number.");
                }
                byte msgId = msg.readByte();
                final NettyMessage decodedMsg;
                switch (msgId) {
                    case ScaleEventMessage.ID:
                        decodedMsg = ScaleEventMessage.readFrom(msg);
                        break;
                    case ScaleBufferMessage.ID:
                        decodedMsg = ScaleBufferMessage.readFrom(msg);
                        break;
                    default:
                        throw new IllegalStateException("Received unknown message id: " + msgId);
                }
                return decodedMsg;
            }finally {
                msg.release();
            }
        }
    }




}
