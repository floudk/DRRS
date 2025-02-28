package org.apache.flink.runtime.scale.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.io.network.ScaleNettyMessage;

import org.apache.flink.runtime.scale.state.KeyOrStateID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

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

    // align may be more early than the connection established
    private final Map<Integer,CompletableFuture<Void>> channelConnectionFutures = new HashMap<>();


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

    public void requestStatesWithCreatingChannel(int targetTaskIndex, ScaleEvent event) throws InterruptedException {
        TaskScaleID targetTaskID = new TaskScaleID(jobVertexID, targetTaskIndex);

        Channel newChannel = commManager.connectRemoteTask(targetTaskID, connectionIDs.get(targetTaskIndex));
        activeChannels.put(targetTaskID, newChannel);
        LOG.info("Established connection with remote task: {}", targetTaskIndex);
        sendRemoteEvent(targetTaskID, event);
    }

    public void onRequestedStatesReceived(int sourceTask, Channel channel){
        TaskScaleID channelID = new TaskScaleID(jobVertexID, sourceTask);
        activeChannels.put(channelID, channel);
        // Complete the pending request if exists, otherwise create a new one
        LOG.info("Established connection from {}", sourceTask);
        synchronized (channelConnectionFutures){
            CompletableFuture<Void> future = channelConnectionFutures.get(sourceTask);
            if (future != null){
                future.complete(null);
            } else {
                channelConnectionFutures.put(sourceTask, CompletableFuture.completedFuture(null));
            }
        }
    }

    public void notifyAlign(Set<Integer> targetTasks){
        synchronized (channelConnectionFutures){
            for (int targetTask : targetTasks){
                CompletableFuture<Void> future = channelConnectionFutures.computeIfAbsent(targetTask, k -> new CompletableFuture<>());
                future.thenRunAsync(() -> {
                    try {
                        sendEvent(targetTask, new ScaleEvent.AlignedAck(ownerTaskIndex));
                    } catch (InterruptedException e) {
                        LOG.error("Failed to send align event to task {}", targetTask);
                    }
                });
            }
        }
    }

    public void sendEvent(int targetTaskIndex, ScaleEvent event) throws InterruptedException {
        TaskScaleID targetTaskID = new TaskScaleID(jobVertexID, targetTaskIndex);
        if (isLocalTo(targetTaskIndex)){
            sendLocalEvent(targetTaskID, event);
        } else {
            checkState(activeChannels.containsKey(targetTaskID), "Channel not established");
            sendRemoteEvent(targetTaskID, event);
        }
    }

    public void sendBuffer(int targetTaskIndex, ScaleBuffer.StateBuffer buffer) throws InterruptedException {
        TaskScaleID targetTaskID = new TaskScaleID(jobVertexID, targetTaskIndex);
        if (isLocalTo(targetTaskIndex)){
            sendLocalBuffer(targetTaskID, buffer);
        } else {
            checkState(activeChannels.containsKey(targetTaskID), "Channel not established");
            sendRemoteBuffer(targetTaskID, buffer);
        }
    }

    private void sendLocalEvent(TaskScaleID targetTaskID, ScaleEvent event){
        commManager.sendEvent(targetTaskID, event, null);
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
    private void sendRemoteBuffer(TaskScaleID receiver, ScaleBuffer.StateBuffer buffer){
        final Channel channel = activeChannels.get(receiver);
        TaskScaleID sender = new TaskScaleID(jobVertexID, ownerTaskIndex);
        KeyOrStateID keyOrStateID = (KeyOrStateID) buffer.outgoingManagedKeyedState.keySet().iterator().next();
        final ScaleNettyMessage.ScaleBufferMessage message = new ScaleNettyMessage.ScaleBufferMessage(
                receiver.vertexID,
                ownerTaskIndex,
                receiver.subtaskIndex,
                buffer.toStateBufferDelegate(
                        channel.alloc(),
                        commManager.serializers.get(sender).typeSerializerGetter,
                        commManager.serializers.get(sender).stateSnapshotTransformerGetter
                ));

        channel.writeAndFlush(message)
                .addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        LOG.error("Failed to send buffer to remote task", future.cause());
                    }
                    LOG.info("Successfully sent buffer {} to remote task {},", keyOrStateID,
                            receiver.subtaskIndex);
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
