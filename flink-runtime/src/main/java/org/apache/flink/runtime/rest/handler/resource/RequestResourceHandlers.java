package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.OperationKey;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.util.SerializedThrowable;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.rest.handler.resource.RequestResourceResult.FAILURE_ALLOCATION_NUM;

public class RequestResourceHandlers
        extends AbstractAsynchronousOperationHandlers<OperationKey, Integer> {

    private abstract class RequestResourceHandlerBase<T extends RequestBody>
            extends TriggerHandler<RestfulGateway, T, RequestResourceTriggerMessageParameters> {

        RequestResourceHandlerBase(
                final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                final Time timeout, Map<String, String> responseHeaders,
                final MessageHeaders<T, TriggerResponse, RequestResourceTriggerMessageParameters> messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        @Override
        protected OperationKey createOperationKey(
                final HandlerRequest<T, RequestResourceTriggerMessageParameters> request) {
            return new OperationKey(new TriggerId());
        }
    }


    public class RequestResourceTriggerHandler
            extends RequestResourceHandlerBase<EmptyRequestBody> {

        public RequestResourceTriggerHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<EmptyRequestBody, TriggerResponse, RequestResourceTriggerMessageParameters> messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        @Override
        protected CompletableFuture<Integer> triggerOperation(
                HandlerRequest<EmptyRequestBody, RequestResourceTriggerMessageParameters> request,
                RestfulGateway gateway)
                throws RestHandlerException {
            Optional<JobID> jobId;
            if (request.getQueryParameter(JobIDQueryParameter.class).size() > 0) {
                jobId = Optional.of(request.getQueryParameter(JobIDQueryParameter.class).get(0));
            } else {
                jobId = Optional.empty();
            }

            boolean resourcesCanBeShared = true;
            if (request.getQueryParameter(ResourcesCanBeSharedQueryParameter.class).size() > 0) {
                resourcesCanBeShared = request.getQueryParameter(ResourcesCanBeSharedQueryParameter.class).get(0);
            }

            // For now, the rescaling interface is not implemented for jobs. A job can only be restarted
            // (in which case its job ID changes), so it is meaningless to attach resources to a running
            // job (identified by a job ID).
            if (jobId.isPresent()) {
                throw new RestHandlerException(
                        "Currently rescaling interface is not supported. " +
                                "So resources cannot be attached to a specific job.",
                        HttpResponseStatus.BAD_REQUEST);
            }

            if (!resourcesCanBeShared) {
                throw new RestHandlerException(
                        "Currently rescaling interface is not supported. " +
                                "So resources can only be used by new jobs, and they will be shared.",
                        HttpResponseStatus.BAD_REQUEST);
            }

            // TODO Replace the two if's with the following if when rescaling interface is implemented.
            /*
            if (!resourcesCanBeShared && !jobId.isPresent()) {
                throw new RestHandlerException(
                        "JobID should be specified when resourcesCanBeShared is set to false",
                        HttpResponseStatus.BAD_REQUEST);
            }
            */

            boolean noTimeOut = true;
            if (request.getQueryParameter(NoTimeOutQueryParameter.class).size() > 0) {
                noTimeOut = request.getQueryParameter(NoTimeOutQueryParameter.class).get(0);
            }

            if (request.getQueryParameter(NumSlotsQueryParameter.class).size() > 0) {
                int numSlots = request.getQueryParameter(NumSlotsQueryParameter.class).get(0);
                return gateway.requestResourceWithSlots(
                        jobId,
                        resourcesCanBeShared,
                        noTimeOut,
                        numSlots,
                        RpcUtils.INF_TIMEOUT);
            } else if (request.getQueryParameter(NumTaskManagersQueryParameter.class).size() > 0) {
                int numTaskManagers = request.getQueryParameter(NumTaskManagersQueryParameter.class).get(0);
                return gateway.requestResourceWithTaskManagers(
                        jobId,
                        resourcesCanBeShared,
                        noTimeOut,
                        numTaskManagers,
                        RpcUtils.INF_TIMEOUT);
            } else {
                throw new RestHandlerException("Either num slots or num task managers should be supplied.",
                        HttpResponseStatus.BAD_REQUEST);
            }
        }
    }

    public class RequestResourceStatusHandler
            extends StatusHandler<RestfulGateway, RequestResourceResult, RequestResourceStatusParameters> {

        public RequestResourceStatusHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<
                        EmptyRequestBody,
                        AsynchronousOperationResult<RequestResourceResult>,
                        RequestResourceStatusParameters> messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        @Override
        protected OperationKey getOperationKey(
                HandlerRequest<EmptyRequestBody, RequestResourceStatusParameters> request) {
            TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
            return new OperationKey(triggerId);
        }

        @Override
        protected RequestResourceResult exceptionalOperationResultResponse(Throwable throwable) {
            return new RequestResourceResult(FAILURE_ALLOCATION_NUM, new SerializedThrowable(throwable));
        }

        @Override
        protected RequestResourceResult operationResultResponse(Integer allocatedResources) {
            return new RequestResourceResult(allocatedResources, null);
        }
    }

}
