package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class RemoveFromBlackListTriggerHeaders implements MessageHeaders<
        EmptyRequestBody,
        EmptyResponseBody,
        RemoveFromBlackListTriggerMessageParameters> {

    private static final RemoveFromBlackListTriggerHeaders INSTANCE = new RemoveFromBlackListTriggerHeaders();

    private static final String URL = String.format(
            "/jobs/:%s/removeFromBlacklist",
            JobIDPathParameter.KEY);

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<EmptyResponseBody> getResponseClass() {
        return EmptyResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Remove the supplied list of hosts from the blacklist.";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public RemoveFromBlackListTriggerMessageParameters getUnresolvedMessageParameters() {
        return new RemoveFromBlackListTriggerMessageParameters();
    }

    public static RemoveFromBlackListTriggerHeaders getInstance() {
        return INSTANCE;
    }
}
