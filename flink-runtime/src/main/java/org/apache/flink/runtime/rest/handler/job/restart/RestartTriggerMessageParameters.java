package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class RestartTriggerMessageParameters extends JobMessageParameters {

    private final HostsQueryParameter HOSTS_QUERY_PARAMETER = new HostsQueryParameter();

	private final RemoveHostsAfterRestartQueryParameter REMOVE_HOSTS_QUERY_PARAMETER =
		new RemoveHostsAfterRestartQueryParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Collections.singletonList(jobPathParameter);
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
		List<MessageQueryParameter<?>> messageQueryParameters = new ArrayList<>();
		messageQueryParameters.add(HOSTS_QUERY_PARAMETER);
		messageQueryParameters.add(REMOVE_HOSTS_QUERY_PARAMETER);
		return messageQueryParameters;
    }
}
