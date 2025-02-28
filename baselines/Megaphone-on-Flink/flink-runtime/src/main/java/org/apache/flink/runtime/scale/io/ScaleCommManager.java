package org.apache.flink.runtime.scale.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.ScaleEvent;
import org.apache.flink.runtime.scale.io.network.NettyScaleServer;
import org.apache.flink.runtime.scale.io.network.ScaleNettyClient;
import org.apache.flink.runtime.scale.io.network.ScaleNettyMessage;
import org.apache.flink.runtime.scale.io.network.ScaleNettyProtocol;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.StateMap;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 1. Request States:
 *    target ----request----> source
 *    source ----send----> target
 * 2. Reroute states(with confirm barrier):
 *    source ----send----> target
 *
 */

public class ScaleCommManager {
    private static final Logger LOG = LoggerFactory.getLogger(ScaleCommManager.class);

    // Local Consumer
    private final Map<TaskScaleID, ScaleCommListener> scaleCommListeners
            = new ConcurrentHashMap<>(16);

    // Remote Connection
    private NettyConfig nettyConfig;
    private NettyBufferPool bufferPool;
    private ScaleNettyProtocol scaleNettyProtocol;

    private AtomicInteger connectionCount = new AtomicInteger(0);
    private NettyScaleServer scaleNettyServer;
    private ScaleNettyClient scaleNettyClient;

    private final Map<TaskScaleID, Channel> activeChannels = new ConcurrentHashMap<>(16);
    public final Map<TaskScaleID, Serializers> serializers = new ConcurrentHashMap<>(16);

    public ScaleCommManager(NettyConfig nettyConfig) {
        this.nettyConfig = nettyConfig;
    }

    // ----------------- Local Consumer -----------------
    public void registerListener(
            JobVertexID jobVertexID,
            int subtaskIndex,
            ScaleCommListener scaleCommListener,
            TypeSerializer inputSerializer,
            Function<String, StateMap> stateMapGetter,
            Function<String, Tuple3<TypeSerializer,TypeSerializer,TypeSerializer>> typeSerializerGetter,
            Function<String, StateSnapshotTransformer> stateSnapshotTransformerGetter) {
        final TaskScaleID taskScaleID = new TaskScaleID(jobVertexID, subtaskIndex);
        scaleCommListeners.put(taskScaleID, scaleCommListener);
        serializers.put(taskScaleID, new Serializers(
                inputSerializer, stateMapGetter, typeSerializerGetter, stateSnapshotTransformerGetter
        ));
    }

    public void sendEvent(TaskScaleID targetTaskID, ScaleEvent event) {
        ScaleCommListener listener = scaleCommListeners.get(targetTaskID);
        checkNotNull(listener, "No listener for task " + targetTaskID);
        listener.onEvent(event);
    }

    public void sendBuffer(TaskScaleID targetTaskID, ScaleBuffer buffer, int fromTask) {
        ScaleCommListener listener = scaleCommListeners.get(targetTaskID);
        checkNotNull(listener, "No listener for task " + targetTaskID);
        listener.onBuffer(buffer, fromTask);
    }

    // ----------------- Remote Connection -----------------
    public void startConnection() {
        if (connectionCount.incrementAndGet() > 1) {
            return;
        }
        if (bufferPool == null) {
            bufferPool = new NettyBufferPool(ScaleConfig.Instance.NETTY_CHUNK_NUM, ScaleConfig.Instance.NETTY_CHUNK_ORDER);
        }
        if (scaleNettyProtocol == null) {
            scaleNettyProtocol = new ScaleNettyProtocol();
        }
        this.scaleNettyServer = new NettyScaleServer(this, nettyConfig, scaleNettyProtocol);
        this.scaleNettyClient = new ScaleNettyClient(this, nettyConfig, scaleNettyProtocol);
        LOG.info("Successfully start scale communication network...");
    }

    public void stopConnection() {
        if (connectionCount.decrementAndGet() == 0) {
            for (Channel channel : activeChannels.values()) {
                channel.close().addListener(future -> {
                    if (!future.isSuccess()) {
                        LOG.error("Error while closing channel", future.cause());
                    }
                });
            }
            scaleNettyServer.shutdown();
            scaleNettyClient.shutdown();
        }
    }

    public void checkRemoteConnection(TaskScaleID targetTaskID, ConnectionID connectionID) throws InterruptedException {
        if(!activeChannels.containsKey(targetTaskID)){
            Channel channel = scaleNettyClient.connect(connectionID.getAddress()).sync().channel();
            LOG.info("Successfully connect to remote task {} with channel {}", targetTaskID.subtaskIndex, channel);
            activeChannels.put(targetTaskID, channel);
        }
    }

    public Map<TaskScaleID, Channel> getActiveChannels() {
        return activeChannels;
    }
    public NettyBufferPool getBufferPool() {
        return bufferPool;
    }


    public void onRemoteEventMessage(ScaleNettyMessage.ScaleEventMessage sem){
        final TaskScaleID targetTaskScaleID = new TaskScaleID(sem.getJobVertexID(), sem.getReceiver());
        sendEvent(targetTaskScaleID, sem.getEvent());
    }
    public void onRemoteBufferMessage(ScaleNettyMessage.ScaleBufferMessage sbm) throws IOException {
        final TaskScaleID targetTaskScaleID = new TaskScaleID(sbm.getJobVertexID(), sbm.getReceiver());
        ScaleBuffer buffer = sbm.getBuffer();
        if (buffer instanceof ScaleBuffer.StateBufferDelegate) {
            sendBuffer(targetTaskScaleID,
                    ((ScaleBuffer.StateBufferDelegate) buffer).toStateBuffer(
                            serializers.get(targetTaskScaleID).stateMapGetter,
                            serializers.get(targetTaskScaleID).typeSerializerGetter
                    ),
                    sbm.getSender()
            );
        } else {
            sendBuffer(targetTaskScaleID, buffer, sbm.getSender());
        }
    }

    public void registerChannel(TaskScaleID senderID, Channel channel) {
        activeChannels.putIfAbsent(senderID, channel);
    }

    public ByteBufAllocator getByteBufAllocator() {
        return bufferPool;
    }


    public static class Serializers {
        public TypeSerializer inputSerializer;
        public Function<String, StateMap> stateMapGetter;
        public Function<String, Tuple3<TypeSerializer,TypeSerializer,TypeSerializer>> typeSerializerGetter;
        public Function<String, StateSnapshotTransformer> stateSnapshotTransformerGetter;

        public Serializers(
                TypeSerializer inputSerializer,
                Function<String, StateMap> stateMapGetter,
                Function<String, Tuple3<TypeSerializer,TypeSerializer,TypeSerializer>> typeSerializerGetter,
                Function<String, StateSnapshotTransformer> stateSnapshotTransformerGetter) {
            this.inputSerializer = inputSerializer;
            this.stateMapGetter = stateMapGetter;
            this.typeSerializerGetter = typeSerializerGetter;
            this.stateSnapshotTransformerGetter = stateSnapshotTransformerGetter;
        }
    }


}
