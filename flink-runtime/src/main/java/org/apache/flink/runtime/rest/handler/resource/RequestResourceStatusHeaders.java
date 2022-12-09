package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class RequestResourceStatusHeaders
        extends AsynchronousOperationStatusMessageHeaders<RequestResourceResult, RequestResourceStatusParameters> {

    private static final RequestResourceStatusHeaders INSTANCE = new RequestResourceStatusHeaders();

    private static final String URL = String.format(
            "/requestResource/:%s",
            TriggerIdPathParameter.KEY);

    public static RequestResourceStatusHeaders getInstance() {
        return INSTANCE;
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
    public Class<RequestResourceResult> getValueClass() {
        return RequestResourceResult.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Returns the status of a requesting resource operation.";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public RequestResourceStatusParameters getUnresolvedMessageParameters() {
        return new RequestResourceStatusParameters();
    }
}
