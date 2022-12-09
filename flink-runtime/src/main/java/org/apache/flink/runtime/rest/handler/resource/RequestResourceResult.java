package org.apache.flink.runtime.rest.handler.resource;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.SerializedThrowableDeserializer;
import org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.util.SerializedThrowable;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RequestResourceResult implements ResponseBody {

    private static final String FIELD_NAME_REQUEST_ID = "request-id";

    private static final String FIELD_NAME_ALLOCATED_RESOURCES = "allocated-resources";

    public static final int FAILURE_ALLOCATION_NUM = -1;

    private static final String FIELD_NAME_FAILURE_CAUSE = "failure-cause";

    @JsonProperty(FIELD_NAME_ALLOCATED_RESOURCES)
    private final int allocateResources;

    @JsonProperty(FIELD_NAME_FAILURE_CAUSE)
    @JsonSerialize(using = SerializedThrowableSerializer.class)
    @JsonDeserialize(using = SerializedThrowableDeserializer.class)
    @Nullable
    private final SerializedThrowable failureCause;

    @JsonCreator
    public RequestResourceResult(
            @JsonProperty(FIELD_NAME_ALLOCATED_RESOURCES) final int allocateResources,
            @JsonProperty(FIELD_NAME_FAILURE_CAUSE)
            @JsonDeserialize(using = SerializedThrowableDeserializer.class)
            @Nullable final SerializedThrowable failureCause) {
        checkArgument(
                failureCause != null ^ allocateResources != FAILURE_ALLOCATION_NUM,
                "The allocation should either fail (failureCause != null) or succeed (allocatedResources != -1");

        this.allocateResources = allocateResources;
        this.failureCause = failureCause;
    }

    public int getAllocateResources() {
        return allocateResources;
    }

    @Nullable
    public SerializedThrowable getFailureCause() {
        return failureCause;
    }
}
