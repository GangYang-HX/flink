package org.apache.flink.runtime.checkpoint;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;

/**
 * Base class for checkpoint related exceptions.
 */
public class RestoreCheckpointException extends RuntimeException{

	private static final long serialVersionUID = -376327443804655084L;

	private final RestoreFailureReason restoreFailureReason;

	public RestoreCheckpointException(RestoreFailureReason restoreFailureReason) {
		super(restoreFailureReason.getMessage());
		this.restoreFailureReason = Preconditions.checkNotNull(restoreFailureReason);
	}

	public RestoreCheckpointException(RestoreFailureReason restoreFailureReason, Throwable cause) {
		super(
			restoreFailureReason.getMessage(),
			cause == null ? null : new SerializedThrowable(cause));

		this.restoreFailureReason = Preconditions.checkNotNull(restoreFailureReason);
	}

	public RestoreFailureReason getRestoreFailureReason() {
		return restoreFailureReason;
	}
}
