package org.apache.flink.runtime.scale.io.message;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Buffer for rerouting records.
 * The buffer will be sent immediately in 3 cases:
 * 1. The buffer is full( size >= bufferCapacity)
 * 2. The time of the buffer is expired( time >= bufferTime)
 * 3. There is a confirm message as event to send with the buffer.
 */
public abstract class RerouteCache<T>{

    protected static final Logger LOG = LoggerFactory.getLogger(RerouteCache.class);

    protected final long bufferCapacity; // buffer size
    protected final long bufferTimeout; // expired time

    protected long lastUpdateTime; // last update time

    protected final int targetTaskIndex; // target task index

    // if time is expired, send the buffer
    Runnable timeExpiredHandler;

    protected ScheduledExecutorService scheduler;

    public Set<Integer> involvedKeys = new HashSet<>();

    protected RerouteCache(
            int targetTaskIndex,
            Runnable timeExpiredHandler,
            long bufferTimeout,
            long bufferCapacity) {
        this.targetTaskIndex = targetTaskIndex;

        this.timeExpiredHandler = timeExpiredHandler;
        this.bufferTimeout = bufferTimeout;
        this.bufferCapacity = bufferCapacity;

        this.lastUpdateTime = System.currentTimeMillis();
        startTimeoutChecker();
    }


    protected void updateLastUpdateTime() {
        this.lastUpdateTime = System.currentTimeMillis();
    }

    // Method to start the timeout checker
    protected void startTimeoutChecker() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        // Schedule the task at a fixed rate
        scheduler.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastUpdateTime > bufferTimeout) {
                // If time expired, call the handler
                timeExpiredHandler.run();
                // close the scheduler to avoid memory leak
                shutdownScheduler();
            }
        }, bufferTimeout, bufferTimeout, TimeUnit.MILLISECONDS);
    }

    public void shutdownScheduler() {
        scheduler.shutdownNow();
    }


    public ByteBuf getBuffer(){
        throw new IllegalStateException("Should not be called");
    }

    public void initWithPreviousOverflow(T record, RerouteCache oldOne) {
        throw new IllegalStateException("Should not be called");
    }

    public boolean addRecord(T record) {
        throw new IllegalStateException("Should not be called");
    }

    public static class RemoteDelegate extends RerouteCache<Object> {
        public final ByteBuf delegate;
        public RemoteDelegate( ByteBuf delegate) {
            super(-1, null, 0, 0);
            this.delegate = delegate;
        }

        @Override
        protected void startTimeoutChecker() {
            // do nothing
        }
    }
}
