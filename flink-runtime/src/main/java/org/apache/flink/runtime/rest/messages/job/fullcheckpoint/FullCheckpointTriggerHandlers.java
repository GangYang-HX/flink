package org.apache.flink.runtime.rest.messages.job.fullcheckpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.*;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.util.SerializedThrowable;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FullCheckpointTriggerHandlers
	extends AbstractAsynchronousOperationHandlers<AsynchronousJobOperationKey, Long> {

	@Nullable
	private final String defaultFullCheckpointDir;

	public FullCheckpointTriggerHandlers(@Nullable final String defaultFullCheckpointDir) {
		this.defaultFullCheckpointDir = defaultFullCheckpointDir;
	}

	private abstract class FullCheckpointHandlerBase<T extends RequestBody>
		extends TriggerHandler<RestfulGateway, T, FullCheckpointTriggerMessageParameters> {

		FullCheckpointHandlerBase(
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout, Map<String, String> responseHeaders,
			final MessageHeaders<T, TriggerResponse, FullCheckpointTriggerMessageParameters> messageHeaders) {
			super(leaderRetriever, timeout, responseHeaders, messageHeaders);
		}

		@Override
		protected AsynchronousJobOperationKey createOperationKey(
			final HandlerRequest<T, FullCheckpointTriggerMessageParameters> request) {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			return AsynchronousJobOperationKey.of(new TriggerId(), jobId);
		}
	}

	public class FullCheckpointTriggerHandler
		extends FullCheckpointHandlerBase<FullCheckpointRequestBody> {

		public FullCheckpointTriggerHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders) {
			super(leaderRetriever, timeout, responseHeaders, FullCheckpointTriggerHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<Long> triggerOperation(
			HandlerRequest<FullCheckpointRequestBody, FullCheckpointTriggerMessageParameters> request,
			RestfulGateway gateway) throws RestHandlerException {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final String requestedTargetDirectory = request.getRequestBody().getTargetDirectory();

			if (requestedTargetDirectory == null && defaultFullCheckpointDir == null) {
				throw new RestHandlerException(
					String.format("Config key [%s] is not set. Property [%s] must be provided.",
						CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
						FullCheckpointRequestBody.FIELD_NAME_TARGET_DIRECTORY),
					HttpResponseStatus.BAD_REQUEST);
			}
			final boolean incremental = request.getRequestBody().isIncremental();
			final String targetDirectory = requestedTargetDirectory != null ? requestedTargetDirectory : defaultFullCheckpointDir;
			return gateway.triggerFullCheckpoint(jobId, RpcUtils.INF_TIMEOUT, incremental, targetDirectory);
		}
	}

	public class FullCheckpointStatusHandler
		extends StatusHandler<RestfulGateway, FullCheckpointInfo, FullCheckpointStatusMessageParameters> {

		public FullCheckpointStatusHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders) {
			super(leaderRetriever, timeout, responseHeaders, FullCheckpointStatusHeaders.getInstance());
		}

		@Override
		protected AsynchronousJobOperationKey getOperationKey(
			HandlerRequest<EmptyRequestBody, FullCheckpointStatusMessageParameters> request) {
			TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
			JobID jobID = request.getPathParameter(JobIDPathParameter.class);
			return AsynchronousJobOperationKey.of(triggerId, jobID);
		}

		@Override
		protected FullCheckpointInfo exceptionalOperationResultResponse(Throwable throwable) {
			return new FullCheckpointInfo(null, new SerializedThrowable(throwable));
		}

		@Override
		protected FullCheckpointInfo operationResultResponse(Long operationResult) {
			return new FullCheckpointInfo(operationResult, null);
		}
	}
}
