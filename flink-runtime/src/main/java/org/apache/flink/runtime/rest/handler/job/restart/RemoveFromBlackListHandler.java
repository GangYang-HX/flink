package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RemoveFromBlackListHandler extends AbstractRestHandler<
        RestfulGateway,
        EmptyRequestBody,
        EmptyResponseBody,
        RemoveFromBlackListTriggerMessageParameters> {

    public RemoveFromBlackListHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<
                    EmptyRequestBody,
                    EmptyResponseBody,
                    RemoveFromBlackListTriggerMessageParameters> messageHeaders) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<EmptyResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody, RemoveFromBlackListTriggerMessageParameters> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
		List<String> blacklistedHosts = request.getQueryParameter(HostsQueryParameter.class);
		blacklistedHosts = blacklistedHosts.stream().filter(host -> !"".equals(host)).collect(Collectors.toList());
		if (blacklistedHosts.isEmpty()) {
            throw new RestHandlerException("Hosts must be supplied!", HttpResponseStatus.BAD_REQUEST);
        }
		JobID jobId = request.getPathParameter(JobIDPathParameter.class);
		log.info("Receive a remove blacklistedHosts request for job {} with blacklistedHosts {}", jobId, blacklistedHosts);

		gateway.removeFromBlackList(jobId, blacklistedHosts);
		return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
    }
}
