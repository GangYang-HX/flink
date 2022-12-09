package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.Collection;
import java.util.Collections;

public class RemoveFromBlackListTriggerMessageParameters
        extends JobMessageParameters {

    private final HostsQueryParameter HOSTS_QUERY_PARAMETER = new HostsQueryParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Collections.singletonList(jobPathParameter);
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.singletonList(HOSTS_QUERY_PARAMETER);
    }
}
