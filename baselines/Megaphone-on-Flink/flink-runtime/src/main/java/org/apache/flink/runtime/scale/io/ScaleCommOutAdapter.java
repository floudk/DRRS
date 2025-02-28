package org.apache.flink.runtime.scale.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.io.message.local.StateBuffer;
import org.apache.flink.runtime.scale.io.network.ScaleNettyMessage;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Owned by the SubScaleCoordinator.
 * This class is responsible for handling the communication between different tasks.
 * If the target task is local(i.e. on the same machine), it will directly access ScaleCommAdapter,
 * otherwise, it will use the Netty to send the message to the target task.
 */
public class ScaleCommOutAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ScaleCommOutAdapter.class);

    private final int ownerTaskIndex; // The taskID of the owner of this ScaleCommAdapter
    private final JobVertexID jobVertexID; // The jobVertexID of the owner of this ScaleCommAdapter

    // Local communication
    private final ScaleCommManager commManager;
    private final Map<TaskScaleID, Channel> activeChannels;

    private List<ConnectionID> connectionIDs;

    public ScaleCommOutAdapter(int ownerTaskIndex, JobVertexID jobVertexID, ScaleCommManager commManager){
        this.ownerTaskIndex = ownerTaskIndex;
        this.jobVertexID = jobVertexID;
        this.commManager = commManager;
        this.activeChannels = commManager.getActiveChannels();
    }

    public TypeSerializer getInputSerializer() {
        return commManager.serializers.get(
                new TaskScaleID(jobVertexID, ownerTaskIndex)).inputSerializer;
    }

    public void reset(List<ConnectionID> connectionIDs) {
        this.connectionIDs = connectionIDs;
    }


    public void sendEvent(int targetTaskIndex, ScaleEvent event) throws InterruptedException {
        TaskScaleID targetTaskID = new TaskScaleID(jobVertexID, targetTaskIndex);
        if (isLocalTo(targetTaskIndex)){
            sendLocalEvent(targetTaskID, event);
        } else {
            commManager.checkRemoteConnection(targetTaskID, connectionIDs.get(targetTaskIndex));
            sendRemoteEvent(targetTaskID, event);
        }
    }

    public void sendBuffer(int targetTaskIndex, ScaleBuffer buffer) throws InterruptedException {
        TaskScaleID targetTaskID = new TaskScaleID(jobVertexID, targetTaskIndex);
        if (isLocalTo(targetTaskIndex)){
            sendLocalBuffer(targetTaskID, buffer);
        } else {
            commManager.checkRemoteConnection(targetTaskID, connectionIDs.get(targetTaskIndex));
            sendRemoteBuffer(targetTaskID, buffer);
        }
    }

    private void sendLocalEvent(TaskScaleID targetTaskID, ScaleEvent event){
        commManager.sendEvent(targetTaskID, event);
    }
    private void sendLocalBuffer(TaskScaleID targetTaskID, ScaleBuffer buffer){
        commManager.sendBuffer(targetTaskID, buffer, ownerTaskIndex);
    }
    private void sendRemoteEvent(TaskScaleID targetTaskID, ScaleEvent event){
        final ScaleNettyMessage.ScaleEventMessage message =
                new ScaleNettyMessage.ScaleEventMessage(
                        targetTaskID.vertexID,ownerTaskIndex, targetTaskID.subtaskIndex, event);
        activeChannels.get(targetTaskID).writeAndFlush(message)
                .addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        LOG.error("Failed to send event to remote task", future.cause());
                    }
                });
    }
    private void sendRemoteBuffer(TaskScaleID receiver, ScaleBuffer buffer){
        final Channel channel = activeChannels.get(receiver);
        TaskScaleID sender = new TaskScaleID(jobVertexID, ownerTaskIndex);

        final ScaleNettyMessage.ScaleBufferMessage message;
        if (buffer instanceof StateBuffer){
            message = new ScaleNettyMessage.ScaleBufferMessage(
                    receiver.vertexID,
                    ownerTaskIndex,
                    receiver.subtaskIndex,
                    ((StateBuffer) buffer).toStateBufferDelegate(
                            channel.alloc(),
                            commManager.serializers.get(sender).typeSerializerGetter,
                            commManager.serializers.get(sender).stateSnapshotTransformerGetter
                    ));
        }else{
            LOG.error("Unknown buffer type: {}", buffer.getClass().getName());
            throw new IllegalArgumentException("Unknown buffer type" + buffer.getClass().getName());
        }
        activeChannels.get(receiver).writeAndFlush(message)
                .addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        LOG.error("Failed to send buffer to remote task", future.cause());
                    }
                });
    }


    public boolean isLocalTo(int targetTaskID){
        checkArgument(targetTaskID != ownerTaskIndex, "Should not send message to itself");
        ResourceID local = connectionIDs.get(ownerTaskIndex).getResourceID();
        ResourceID target = connectionIDs.get(targetTaskID).getResourceID();
        return local.equals(target);
    }

    public ByteBufAllocator getByteBufAllocator() {
        return commManager.getByteBufAllocator();
    }

    public TypeSerializer getInputSerializer(int subtaskIndex) {
        return commManager.serializers.get(
                new TaskScaleID(jobVertexID, subtaskIndex)).inputSerializer;
    }

    public int getRemoteTaskNum() {
        int count = 0;
        for (int i = 0; i < connectionIDs.size(); i++) {
            if (i == ownerTaskIndex) {
                continue;
            }
            if (!isLocalTo(i)) {
                count++;
            }
        }
        return count;
    }
}
