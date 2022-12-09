package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import java.util.Collection;
import java.util.Collections;

public class RequestResourceStatusParameters extends JobMessageParameters {

    public TriggerIdPathParameter triggerId = new TriggerIdPathParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Collections.singletonList(triggerId);
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.emptyList();
    }
}
