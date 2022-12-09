package org.apache.flink.runtime.rest.messages.job.fullcheckpoint;

import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.Collection;
import java.util.Collections;

/**
 * The parameters for triggering a full checkpoint.
 */
public class FullCheckpointTriggerMessageParameters extends MessageParameters {

	public JobIDPathParameter jobId = new JobIDPathParameter();

	@Override
	public Collection<MessagePathParameter<?>> getPathParameters() {
		return Collections.singleton(jobId);
	}

	@Override
	public Collection<MessageQueryParameter<?>> getQueryParameters() {
		return Collections.emptyList();
	}
}
