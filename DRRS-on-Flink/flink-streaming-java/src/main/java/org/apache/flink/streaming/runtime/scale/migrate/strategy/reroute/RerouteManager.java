package org.apache.flink.streaming.runtime.scale.migrate.strategy.reroute;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.io.ScaleCommOutAdapter;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.scale.io.message.RerouteCache;
import org.apache.flink.runtime.scale.io.message.barrier.ConfirmBarrier;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Responsible for rerouting records to the new target task:
 * 1. re-route the records that belong to the key-group that is migrated to the new target task.
 * 2. re-route the confirm barrier to the new target task.
 */
public class RerouteManager {
    protected static final Logger LOG = LoggerFactory.getLogger(RerouteManager.class);

    private final ScalingContext scalingContext;
    private final ScaleCommOutAdapter scaleCommOutAdapter;
    private final int subscaleID;

    private RerouteCache rerouteBuffer;
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    protected final Thread taskExecutor;
    private int rerouteCount = 0;
    AtomicInteger pendingTasks = new AtomicInteger(0);


    private final Map<Integer, CompletableFuture<Void>> stateConsumedFutures = new ConcurrentHashMap<>();

    public final CompletableFuture<Void> noMoreBarriersFuture = new CompletableFuture<>();

    public RerouteManager(
            ScalingContext scalingContext,
            ScaleCommOutAdapter scaleCommOutAdapter,
            int subscaleID) {
        this.scalingContext = scalingContext;
        this.scaleCommOutAdapter = scaleCommOutAdapter;
        this.subscaleID = subscaleID;

        // Create a single task executor thread
        taskExecutor = new Thread(this::processTasks);
        taskExecutor.start();

        registerClose();
    }
    protected void addTask(Runnable task) {
        pendingTasks.incrementAndGet();
        try {
            taskQueue.put(task); // Ensure tasks are added in FIFO order
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Error while adding task to queue", e);
        }
    }

    private void processTasks() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Runnable task = taskQueue.take(); // Block and wait for tasks
                task.run(); // Execute the task
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                pendingTasks.decrementAndGet();
            }
        }
    }

    public void registerClose() {
        noMoreBarriersFuture.thenRun(() -> {
            LOG.info("RerouteManager {} is closing", subscaleID);
            // Wait for all tasks to complete
            while (pendingTasks.get() > 0) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting for reroute tasks to complete", e);
                    Thread.currentThread().interrupt();
                }
            }
            rerouteBuffer.shutdownScheduler();
            taskExecutor.interrupt(); // Stop the task executor thread
        });
    }


    public void rerouteConfirmBarrier(
            ConfirmBarrier cb,
            Set<Integer> targetTasks,
            InputChannelInfo channelInfo,
            CompletableFuture<Void> consumeFuture) {
        for(int targetTask : targetTasks){
            addTask(() -> rerouteCBInternal(targetTask,  channelInfo, cb, consumeFuture));
        }
    }

    protected void rerouteCBInternal(
            int targetTask,
            InputChannelInfo channelInfo,
            ConfirmBarrier cb,
            CompletableFuture<Void> consumeFuture) {
        RerouteCache cacheToReroute = rerouteBuffer;
        this.rerouteBuffer = null;
        reroute(targetTask, cacheToReroute, channelInfo, cb, consumeFuture);
    }

    public <IN> void rerouteRecord(StreamRecord<IN> record, int keyGroupIndex) {
        int targetTask = scalingContext.getTargetTask(keyGroupIndex);
        addTask(() -> rerouteRecordInternal(record, targetTask, keyGroupIndex));
    }

    protected void rerouteRecordInternal(StreamRecord record, int targetTask, int keyGroupIndex) {
        if (rerouteBuffer == null) {
            rerouteBuffer = createNewRerouteBuffer(targetTask);
        }

        if (!rerouteBuffer.addRecord(record)){
            RerouteCache newCache = createNewRerouteBuffer(targetTask);
            newCache.initWithPreviousOverflow(record, rerouteBuffer);
            newCache.involvedKeys.add(keyGroupIndex);
            RerouteCache cacheToReroute = rerouteBuffer;
            this.rerouteBuffer = newCache;
            reroute(targetTask, cacheToReroute, null, null, null);
        }else{
            rerouteBuffer.involvedKeys.add(keyGroupIndex);
        }
    }

    public void reroute(
            int targetTask,
            @Nullable RerouteCache rerouteCache,
            @Nullable InputChannelInfo channelInfo,
            @Nullable ConfirmBarrier confirmBarrier,
            @Nullable CompletableFuture<Void> channelConsumeFuture) {

        CompletableFuture statesConsumedFuture;
        if (rerouteCache != null) {
            rerouteCache.shutdownScheduler();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            rerouteCache.involvedKeys
                    .forEach(keyGroupIndex -> futures.add(stateConsumedFutures.get(keyGroupIndex)));
            statesConsumedFuture = FutureUtils.combineAll(futures);
        }else{
            statesConsumedFuture = CompletableFuture.completedFuture(null);
        }

        ScaleBuffer.RecordBuffer recordBuffer =
                new ScaleBuffer.RecordBuffer(
                        subscaleID,
                        getAndIncrementRerouteCount(targetTask),
                        rerouteCache,
                        confirmBarrier != null,
                        channelInfo,
                        confirmBarrier == null ? null : confirmBarrier.getInvolvedKeys());

        statesConsumedFuture.thenRunAsync(() -> {
            scaleCommOutAdapter.sendRerouteBuffer(targetTask, recordBuffer, subscaleID);
            if (channelConsumeFuture != null) {
                channelConsumeFuture.complete(null);
            }
        });
    }

    protected int getAndIncrementRerouteCount(int targetTask) {
        return rerouteCount++;
    }

    protected RerouteCache createNewRerouteBuffer(int targetTaskIndex) {
        return new RemoteRerouteCache(
                targetTaskIndex,
                () -> addTask(() -> {
                            RerouteCache cacheToReroute = rerouteBuffer;
                            this.rerouteBuffer = null;
                            reroute(targetTaskIndex, cacheToReroute, null, null, null);}),
                ScaleConfig.Instance.REROUTE_BUFFER_TIMEOUT,
                ScaleConfig.Instance.REROUTE_BUFFER_REMOTE_SIZE,
                scaleCommOutAdapter.getByteBufAllocator(),
                scaleCommOutAdapter.getInputSerializer(scalingContext.getSubtaskIndex()).duplicate());

//        if (scaleCommOutAdapter.isLocalTo(targetTaskIndex)) {
//            return new LocalRerouteCache(
//                    targetTaskIndex,
//                    () -> addTask(targetTaskIndex,
//                            () -> reroute(targetTaskIndex, buffers.remove(targetTaskIndex),
//                                    null, null, null)),
//                    ScaleConfig.Instance.REROUTE_BUFFER_TIMEOUT,
//                    ScaleConfig.Instance.REROUTE_BUFFER_LOCAL_SIZE);
//        } else {
//
//        }
    }
    public void setStateConsumeFuture(Integer keygroup, CompletableFuture<Void> adapterConsumeFuture) {
        stateConsumedFutures.put(keygroup, adapterConsumeFuture);
    }
}
