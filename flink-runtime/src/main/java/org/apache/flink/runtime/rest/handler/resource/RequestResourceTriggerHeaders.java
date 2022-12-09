package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

public class RequestResourceTriggerHeaders
        extends AsynchronousOperationTriggerMessageHeaders<EmptyRequestBody, RequestResourceTriggerMessageParameters> {

    private static final RequestResourceTriggerHeaders INSTANCE = new RequestResourceTriggerHeaders();


    private static final String URL = "/requestResource";

    private RequestResourceTriggerHeaders() {
    }

    public static RequestResourceTriggerHeaders getInstance() {
        return INSTANCE;
    }

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
        return "Request the specified resources (slots or task managers) from the cluster " +
                "manager (e.g. YARN, k8s), and keep it for long enough.";
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
    public RequestResourceTriggerMessageParameters getUnresolvedMessageParameters() {
        return new RequestResourceTriggerMessageParameters();
    }
}
