package org.apache.flink.runtime.rest.messages.job.fullcheckpoint;

import org.apache.flink.runtime.rest.messages.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class FullCheckpointStatusMessageParameters extends MessageParameters {

	public final JobIDPathParameter jobIdPathParameter = new JobIDPathParameter();

	public final TriggerIdPathParameter triggerIdPathParameter = new TriggerIdPathParameter();

	@Override
	public Collection<MessagePathParameter<?>> getPathParameters() {
		return Arrays.asList(jobIdPathParameter, triggerIdPathParameter);
	}

	@Override
	public Collection<MessageQueryParameter<?>> getQueryParameters() {
		return Collections.emptyList();
	}
}
