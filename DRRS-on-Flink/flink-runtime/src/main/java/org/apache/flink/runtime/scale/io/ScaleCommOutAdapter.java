package org.apache.flink.runtime.scale.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.ScalingContext;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

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

    private final Map<SubscaleChannelID, Channel> remoteChannels = new ConcurrentHashMap<>();
    private final Map<SubscaleChannelID, CompletableFuture<Void>> pendingRequests = new ConcurrentHashMap<>();
    private final Map<SubscaleChannelID, ChannelCloseHelper> channelCloseHelper = new ConcurrentHashMap<>();

    private List<ConnectionID> connectionIDs;
    public ScalingContext scalingContext;

    public ScaleCommOutAdapter(int ownerTaskIndex, JobVertexID jobVertexID, ScaleCommManager commManager) {
        this.ownerTaskIndex = ownerTaskIndex;
        this.jobVertexID = jobVertexID;
        this.commManager = commManager;
    }

    public TypeSerializer getInputSerializer() {
        return commManager.serializers.get(
                new TaskScaleID(jobVertexID, ownerTaskIndex)).inputSerializer;
    }

    public void reset(List<ConnectionID> connectionIDs) {
        this.connectionIDs = connectionIDs;
    }


    public void requestStatesWithCreatingChannel(
            int tartTask,
            List<Integer> requestingKeys,
            int subscaleID,
            int expectedFirstKey) throws InterruptedException {
        SubscaleChannelID channelID = new SubscaleChannelID(subscaleID, tartTask);
        Channel channel = commManager.connectRemoteTask(new TaskScaleID(jobVertexID, tartTask), connectionIDs.get(tartTask));
        remoteChannels.put(channelID, channel);
        LOG.info("Established connection with remote task: {}-{}", tartTask, subscaleID);
        ScaleNettyMessage.ScaleEventMessage message = new ScaleNettyMessage.ScaleEventMessage(
                jobVertexID, ownerTaskIndex, tartTask, new ScaleEvent.StateRequestEvent(ownerTaskIndex, requestingKeys, subscaleID,expectedFirstKey));
        sendMessage(channel, message, "request state", false);
        channelCloseHelper.put(channelID, new ChannelCloseHelper(requestingKeys.size()));
    }
    public void onRequestedStatesReceived(int subscaleID, int sourceTask, Channel channel){

        SubscaleChannelID channelID = new SubscaleChannelID(subscaleID, sourceTask);
        remoteChannels.put(channelID, channel);
        // Complete the pending request if exists, otherwise create a new one
        pendingRequests.computeIfAbsent(channelID, k -> new CompletableFuture<>()).complete(null);
        LOG.info("Established connection: {}-{}", channelID, System.identityHashCode(pendingRequests.get(channelID)));
    }

    public void ackStateReceived(int ackTask, int keyGroupIndex, int subscaleID, int expectedNext) {
        SubscaleChannelID channelID = new SubscaleChannelID(subscaleID, ackTask);
        Channel channel = remoteChannels.get(channelID);
        checkNotNull(channel, "Channel to " + ackTask + "-" + subscaleID + " is not established");
        ScaleNettyMessage.ScaleEventMessage message = new ScaleNettyMessage.ScaleEventMessage(
                jobVertexID,
                ownerTaskIndex,
                ackTask,
                new ScaleEvent.AcknowledgeStates(ownerTaskIndex, keyGroupIndex, expectedNext));
        LOG.info("Send acknowledge state to remote task: {}", ackTask);
        sendMessage(channel, message, "acknowledge state", true);
        channelCloseHelper.get(channelID).decrease();
    }

    public void sendStateBuffer(int targetTaskIndex, StateBuffer stateBuffer, int subscaleID) {
        SubscaleChannelID channelID = new SubscaleChannelID(subscaleID, targetTaskIndex);
        Channel channel = remoteChannels.get(channelID);

        try {
            TaskScaleID sender = new TaskScaleID(jobVertexID, ownerTaskIndex);
            final ScaleNettyMessage.ScaleBufferMessage message= new ScaleNettyMessage.ScaleBufferMessage(
                    jobVertexID,
                    ownerTaskIndex,
                    targetTaskIndex,
                    stateBuffer.toStateBufferDelegate(
                            channel.alloc(),
                            commManager.serializers.get(sender).typeSerializerGetter,
                            commManager.serializers.get(sender).stateSnapshotTransformerGetter
                    ));
            LOG.info("Send state buffer to remote task: {}", targetTaskIndex);
            sendMessage(channel, message, "state buffer", false);
        } catch (Exception e) {
            LOG.error("Failed to serialize state buffer", e);
            throw new RuntimeException("Failed to serialize state buffer", e);
        }
    }

    public void sendRerouteBuffer(int targetTaskIndex, ScaleBuffer.RecordBuffer buffer, int subscaleID) {
        final ScaleNettyMessage.ScaleBufferMessage message =
                new ScaleNettyMessage.ScaleBufferMessage(
                        jobVertexID, ownerTaskIndex, targetTaskIndex, buffer);
        SubscaleChannelID channelID = new SubscaleChannelID(subscaleID, targetTaskIndex);
        CompletableFuture<Void> pendingRequest = pendingRequests.computeIfAbsent(channelID, k -> new CompletableFuture<>());
        LOG.info("Send reroute buffer {} to {} with connection established status {}({}-{})", buffer.sequenceNumber, targetTaskIndex, pendingRequest.isDone(),channelID, System.identityHashCode(pendingRequest));
        pendingRequest.thenAccept(v -> {
            Channel newChannel = remoteChannels.get(channelID);
            checkNotNull(newChannel, "Channel to " + targetTaskIndex + "-" + subscaleID + " is not established");
            LOG.info("Send reroute buffer to remote task: {}, seq {}, involved: {}", targetTaskIndex, buffer.sequenceNumber, buffer.confirmedKeys);
            sendMessage(newChannel, message, "reroute buffer "+targetTaskIndex, false);
        });
    }

    public void closeChannels(int subscaleID) {
        remoteChannels.entrySet().removeIf(entry -> {
            SubscaleChannelID channelID = entry.getKey();
            Channel channel = entry.getValue();
            if (channelID.subscaleID == subscaleID) {
                channelCloseHelper.get(channelID).future.thenRun(() -> {
                    LOG.info("Close channel: {}-{}", channelID.targetTaskIndex, channelID.subscaleID);
                    channel.close().addListener(future -> {
                        if (!future.isSuccess()) {
                            LOG.error("Error while closing channel", future.cause());
                        }
                    });
                });
                return true;
            }
            return false;
        }
        );
    }


    private void sendMessage(Channel channel, ScaleNettyMessage message, String logMessage, boolean isAck) {
        channel.writeAndFlush(message)
                .addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        LOG.error("Failed to send event to remote task", future.cause());
                    }
                    if (isAck) {
                        scalingContext.ackSent();
                    }
                    LOG.info("Successfully sent {} to remote task",logMessage);
                });
    }



    public static class SubscaleChannelID {
        public final int subscaleID;
        public final int targetTaskIndex;
        public SubscaleChannelID(int subscaleID, int targetTaskIndex) {
            this.subscaleID = subscaleID;
            this.targetTaskIndex = targetTaskIndex;
        }
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SubscaleChannelID) {
                SubscaleChannelID other = (SubscaleChannelID) obj;
                return this.subscaleID == other.subscaleID && this.targetTaskIndex == other.targetTaskIndex;
            }
            return false;
        }
        @Override
        public int hashCode() {
            return subscaleID * 31 + targetTaskIndex;
        }
        @Override
        public String toString() {
            return targetTaskIndex + "-" + subscaleID;
        }
    }


    // ------------------------------ local utils ------------------------------
    // ------------------------------ not used for evaluation ------------------------------
    private void sendLocalBuffer(TaskScaleID targetTaskID, ScaleBuffer buffer){
        commManager.sendBuffer(targetTaskID, buffer, ownerTaskIndex);
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
    static class ChannelCloseHelper{
        public int count;
        public CompletableFuture<Void> future;
        public ChannelCloseHelper(int count){
            this.count = count;
            this.future = new CompletableFuture<>();
        }
        public void decrease(){
            count--;
            if(count == 0){
                future.complete(null);
            }else{
                LOG.info("Left {} pending acks", count);
            }
        }

    }

}
