package org.apache.flink.runtime.rest.messages.job.fullcheckpoint;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * These headers define the protocol for triggering a full checkpoint.
 */
public class FullCheckpointTriggerHeaders
	extends AsynchronousOperationTriggerMessageHeaders<FullCheckpointRequestBody, FullCheckpointTriggerMessageParameters> {

	private static final FullCheckpointTriggerHeaders INSTANCE = new FullCheckpointTriggerHeaders();

	private static final String URL = String.format(
		"/jobs/:%s/full-checkpoint",
		JobIDPathParameter.KEY);

	private FullCheckpointTriggerHeaders() {
	}

	@Override
	public Class<FullCheckpointRequestBody> getRequestClass() {
		return FullCheckpointRequestBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.ACCEPTED;
	}

	@Override
	public FullCheckpointTriggerMessageParameters getUnresolvedMessageParameters() {
		return new FullCheckpointTriggerMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static FullCheckpointTriggerHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	protected String getAsyncOperationDescription() {
		return "Triggers a full checkpoint.";
	}
}
