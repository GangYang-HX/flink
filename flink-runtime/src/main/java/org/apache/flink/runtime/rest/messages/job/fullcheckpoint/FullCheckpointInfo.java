package org.apache.flink.runtime.rest.messages.job.fullcheckpoint;

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


/**
 * Represents information about a finished full checkpoint.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FullCheckpointInfo implements ResponseBody {

	private static final String FIELD_NAME_CHECKPOINT_ID = "checkpoint-id";

	private static final String FIELD_NAME_FAILURE_CAUSE = "failure-cause";

	@JsonProperty(FIELD_NAME_CHECKPOINT_ID)
	@Nullable
	private final Long checkpointId;

	@JsonProperty(FIELD_NAME_FAILURE_CAUSE)
	@JsonSerialize(using = SerializedThrowableSerializer.class)
	@JsonDeserialize(using = SerializedThrowableDeserializer.class)
	@Nullable
	private final SerializedThrowable failureCause;

	@JsonCreator
	public FullCheckpointInfo(
		@JsonProperty(FIELD_NAME_CHECKPOINT_ID) @Nullable final Long checkpointId,
		@JsonProperty(FIELD_NAME_FAILURE_CAUSE)
		@JsonDeserialize(using = SerializedThrowableDeserializer.class)
		@Nullable final SerializedThrowable failureCause) {
		checkArgument(
			checkpointId != null ^ failureCause != null,
			"Either checkpointId or failureCause must be set");

		this.checkpointId = checkpointId;
		this.failureCause = failureCause;
	}

	@Nullable
	public Long getCheckpointId() {
		return checkpointId;
	}

	@Nullable
	public SerializedThrowable getFailureCause() {
		return failureCause;
	}
}
