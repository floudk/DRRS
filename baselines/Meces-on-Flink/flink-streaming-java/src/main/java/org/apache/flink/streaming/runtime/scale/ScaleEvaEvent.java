package org.apache.flink.streaming.runtime.scale;

/**
 * Interface for events that are related to the scaling of a task,
 *  just for collecting the subtask index when evaluation.
 */
public abstract class ScaleEvaEvent {

    protected boolean isMarker = false;

    public long creationTime;
    public long outputTime;
    public int processingTask = -1;

    public void setOutTime() {
        outputTime = System.currentTimeMillis();
    }
    public boolean isMarker(){
        return isMarker;
    }

    public void setSubtaskIndex(int subtaskIndex) {
        processingTask = subtaskIndex;
    }
}
