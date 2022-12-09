package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class RestartStatusHeaders
        extends AsynchronousOperationStatusMessageHeaders<RestartResult, RestartStatusMessageParameters> {

    private static final String URL = String.format(
            "/jobs/:%s/restart/:%s",
            JobIDPathParameter.KEY,
            TriggerIdPathParameter.KEY);

    private static final RestartStatusHeaders INSTANCE = new RestartStatusHeaders();

    private RestartStatusHeaders() {
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<RestartResult> getValueClass() {
        return RestartResult.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Get the restart status.";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public RestartStatusMessageParameters getUnresolvedMessageParameters() {
        return new RestartStatusMessageParameters();
    }

    public static RestartStatusHeaders getInstance() {
        return INSTANCE;
    }
}
