package org.apache.flink.runtime.rest.handler.job.restart;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.SerializedThrowableDeserializer;
import org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.util.SerializedThrowable;

import javax.annotation.Nullable;

public class RestartResult implements ResponseBody {

    private static final String FIELD_NAME_RESTART_RESULT = "result";

    private static final String FIELD_NAME_FAILURE_CAUSE = "failureCause";

    @JsonProperty(FIELD_NAME_RESTART_RESULT)
    private final boolean restartResult;

    @JsonProperty(FIELD_NAME_FAILURE_CAUSE)
    @JsonSerialize(using = SerializedThrowableSerializer.class)
    @JsonDeserialize(using = SerializedThrowableDeserializer.class)
    @Nullable
    private final SerializedThrowable failureCause;

    @JsonCreator
    public RestartResult(
            @JsonProperty(FIELD_NAME_RESTART_RESULT) final boolean restartResult,
            @JsonProperty(FIELD_NAME_FAILURE_CAUSE)
            @JsonDeserialize(using = SerializedThrowableDeserializer.class)
            @Nullable final SerializedThrowable failureCause) {
        this.restartResult = restartResult;
        this.failureCause = failureCause;
    }

    public boolean isRestartResult() {
        return restartResult;
    }

    @Nullable
    public SerializedThrowable getFailureCause() {
        return failureCause;
    }
}
