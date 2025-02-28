package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class StateSizeMessageParameters extends MessageParameters {
    public JobIDPathParameter jobIdPathParameter = new JobIDPathParameter();
    public JobVertexIdPathParameter jobVertexIdPathParameter = new JobVertexIdPathParameter();

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.emptyList();
    }
    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Collections.unmodifiableCollection(Arrays.asList(jobIdPathParameter, jobVertexIdPathParameter));
    }
}
