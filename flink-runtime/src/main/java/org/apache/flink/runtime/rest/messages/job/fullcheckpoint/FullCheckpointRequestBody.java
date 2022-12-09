package org.apache.flink.runtime.rest.messages.job.fullcheckpoint;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;


/**
 * {@link RequestBody} for triggering a full checkpoint.
 */
public class FullCheckpointRequestBody implements RequestBody {

	private static final String FIELD_NAME_INCREMENTAL = "incremental";

	public static final String FIELD_NAME_TARGET_DIRECTORY = "targetDirectory";

	@JsonProperty(FIELD_NAME_INCREMENTAL)
	private final boolean incremental;

	@JsonProperty(FIELD_NAME_TARGET_DIRECTORY)
	@Nullable
	private final String targetDirectory;

	@JsonCreator
	public FullCheckpointRequestBody(
		@Nullable @JsonProperty(FIELD_NAME_TARGET_DIRECTORY) final String targetDirectory,
		@JsonProperty(value = FIELD_NAME_INCREMENTAL, defaultValue = "false") final boolean incremental) {
		this.incremental = incremental;
		this.targetDirectory = targetDirectory;
	}

	public boolean isIncremental() {
		return incremental;
	}

	@Nullable
	public String getTargetDirectory() {
		return targetDirectory;
	}

}
