package org.apache.flink.runtime.scale.io;

import org.apache.flink.runtime.jobgraph.JobVertexID;

public class TaskScaleID {
    public final JobVertexID vertexID;
    public final int subtaskIndex;

    public TaskScaleID(JobVertexID vertexID, int subtaskIndex) {
        this.vertexID = vertexID;
        this.subtaskIndex = subtaskIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskScaleID that = (TaskScaleID) o;
        return subtaskIndex == that.subtaskIndex && vertexID.equals(that.vertexID);
    }

    @Override
    public int hashCode() {
        int result = vertexID.hashCode();
        result = 31 * result + subtaskIndex;
        return result;
    }

    @Override
    public String toString() {
        return vertexID + "[" + subtaskIndex + "]";
    }

}
