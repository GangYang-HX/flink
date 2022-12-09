package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class RestartStatusMessageParameters extends JobMessageParameters {

    private final TriggerIdPathParameter triggerIdPathParameter = new TriggerIdPathParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Arrays.asList(jobPathParameter, triggerIdPathParameter);
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.emptyList();
    }
}
