package org.apache.flink.runtime.rest.messages.job.fullcheckpoint;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import static org.apache.flink.runtime.rest.HttpMethodWrapper.GET;

public class FullCheckpointStatusHeaders
	extends AsynchronousOperationStatusMessageHeaders<FullCheckpointInfo, FullCheckpointStatusMessageParameters> {

	private static final FullCheckpointStatusHeaders INSTANCE = new FullCheckpointStatusHeaders();

	private static final String URL = String.format(
		"/jobs/:%s/full-checkpoint/:%s",
		JobIDPathParameter.KEY,
		TriggerIdPathParameter.KEY);

	private FullCheckpointStatusHeaders() {
	}

	public static FullCheckpointStatusHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	@Override
	public Class<FullCheckpointInfo> getValueClass() {
		return FullCheckpointInfo.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public String getDescription() {
		return "Returns the status of a full checkpoint operation.";
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public FullCheckpointStatusMessageParameters getUnresolvedMessageParameters() {
		return new FullCheckpointStatusMessageParameters();
	}
}
