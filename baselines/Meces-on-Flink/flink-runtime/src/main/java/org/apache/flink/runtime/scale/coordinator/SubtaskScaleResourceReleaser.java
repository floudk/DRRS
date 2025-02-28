package org.apache.flink.runtime.scale.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

public class SubtaskScaleResourceReleaser {
    static private final Logger LOG = LoggerFactory.getLogger(SubtaskScaleResourceReleaser.class);

    private final Runnable scaleCompletionAcknowledger;

    //  the release callback stack
    private final Stack<Runnable> releaseCallbacks = new Stack<>();

    public SubtaskScaleResourceReleaser(Runnable scaleCompletionAcknowledger) {
        this.scaleCompletionAcknowledger = scaleCompletionAcknowledger;
    }

    public void registerReleaseCallback(Runnable callback) {
        // register the callback
        releaseCallbacks.push(callback);
    }

    public void release() {
        // release the resources
        while (!releaseCallbacks.isEmpty()) {
            try {
                releaseCallbacks.pop().run();
            } catch (Exception e) {
                LOG.error("Error while releasing resources", e);
            }
        }
    }

    public void notifyComplete() {
        scaleCompletionAcknowledger.run();
        // notify the completion and release the resources
    }
}
