package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class RestartTriggerHeaders
        extends AsynchronousOperationTriggerMessageHeaders<EmptyRequestBody, RestartTriggerMessageParameters> {

    private static final RestartTriggerHeaders INSTANCE = new RestartTriggerHeaders();

    private static final String URL = String.format(
            "/jobs/:%s/restart",
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
    protected String getAsyncOperationDescription() {
        return "Asynchronously trigger restarting of a job.";
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public RestartTriggerMessageParameters getUnresolvedMessageParameters() {
        return new RestartTriggerMessageParameters();
    }

    public static RestartTriggerHeaders getInstance() {
        return INSTANCE;
    }
}
