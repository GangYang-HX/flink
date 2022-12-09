package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.SerializedThrowable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RestartHandlers
        extends AbstractAsynchronousOperationHandlers<AsynchronousJobOperationKey, Boolean> {

    public abstract class RestartHandlerBase
            extends TriggerHandler<RestfulGateway, EmptyRequestBody, RestartTriggerMessageParameters> {

        protected RestartHandlerBase(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<EmptyRequestBody, TriggerResponse, RestartTriggerMessageParameters> messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        @Override
        protected AsynchronousJobOperationKey createOperationKey(
                HandlerRequest<EmptyRequestBody, RestartTriggerMessageParameters> request) {
            JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return AsynchronousJobOperationKey.of(new TriggerId(), jobId);
        }
    }

    public class RestartTriggerHandler extends RestartHandlerBase {

        public RestartTriggerHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<EmptyRequestBody, TriggerResponse, RestartTriggerMessageParameters> messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        @Override
        protected CompletableFuture<Boolean> triggerOperation(
                HandlerRequest<EmptyRequestBody, RestartTriggerMessageParameters> request,
                RestfulGateway gateway) throws RestHandlerException {
            JobID jobId = request.getPathParameter(JobIDPathParameter.class);

            List<String> blacklistedHosts = request.getQueryParameter(HostsQueryParameter.class);
			List<Boolean> removeHostsAfterRestarts =
				request.getQueryParameter(RemoveHostsAfterRestartQueryParameter.class);
			// default remove hosts
			boolean removeHostsAfterRestart = true;
			if (!removeHostsAfterRestarts.isEmpty()) {
				removeHostsAfterRestart = removeHostsAfterRestarts.get(0);
			}

			blacklistedHosts = blacklistedHosts.stream().filter(host -> !"".equals(host)).collect(Collectors.toList());
			log.info("Receive a restart request for job {} with blacklistedHosts {}, removeHostsAfterRestart {}", jobId,
				blacklistedHosts, removeHostsAfterRestart);

			return gateway.restartJob(jobId, blacklistedHosts, removeHostsAfterRestart, RpcUtils.INF_TIMEOUT);
        }
    }

    public class RestartStatusHandler
            extends StatusHandler<RestfulGateway, RestartResult, RestartStatusMessageParameters> {
        public RestartStatusHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<
                        EmptyRequestBody,
                        AsynchronousOperationResult<RestartResult>,
                        RestartStatusMessageParameters> messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        @Override
        protected AsynchronousJobOperationKey getOperationKey(
                HandlerRequest<EmptyRequestBody, RestartStatusMessageParameters> request) {
            JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
            return AsynchronousJobOperationKey.of(triggerId, jobId);
        }

        @Override
        protected RestartResult exceptionalOperationResultResponse(Throwable throwable) {
            return new RestartResult(false, new SerializedThrowable(throwable));
        }

        @Override
        protected RestartResult operationResultResponse(Boolean operationResult) {
            return new RestartResult(operationResult, null);
        }
    }
}
