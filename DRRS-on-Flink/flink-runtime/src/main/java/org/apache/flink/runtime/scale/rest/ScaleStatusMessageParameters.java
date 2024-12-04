package org.apache.flink.runtime.scale.rest;

import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class ScaleStatusMessageParameters extends MessageParameters {

    public final JobIDPathParameter jobIdPathParameter = new JobIDPathParameter();

    public final TriggerIdPathParameter triggerIdPathParameter = new TriggerIdPathParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Collections.unmodifiableCollection(
                Arrays.asList(jobIdPathParameter, triggerIdPathParameter));
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.emptyList();
    }
}
