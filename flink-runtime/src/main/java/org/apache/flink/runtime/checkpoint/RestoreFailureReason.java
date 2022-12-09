package org.apache.flink.runtime.checkpoint;

/**
 * Various reasons why restore from checkpoint was failure.
 */
public enum RestoreFailureReason {
	/* Exception when load metadata file. */
	LOAD_METADATA_EXCEPTION("Load metadata file exception"),

	/* Exception when map checkpoint state to task. */
	OPERATOR_STATE_MISMATCH("Operator state mismatch"),

	/* Exception when maximum parallelism mismatch. */
	MAXIMUM_PARALLELISM_MISMATCH("Maximum parallelism mismatch"),

	/* Exception when keyed state handle is unexpected. */
	UNEXPECTED_STATE_HANDLE_TYPE("Unexpected key state handle type"),

	/* Exception when create and restore key state backend. */
	KEYED_STATE_RESTORED_EXCEPTION("Keyed state create and restore exception"),

	/* Exception when create and restore operator state backend. */
	OPERATOR_STATE_RESTORED_EXCEPTION("Operator state create and restore exception"),

	/* Exception when create and restore state backend. */
	STATE_RESTORED_EXCEPTION("State restore exception"),

	/* the exception when initialize state in restoring */
	INITIALIZE_STATE_EXCEPTION("Initialize state exception"),

	/* Exception when restore inflight data. */
	INFLIGHT_DATA_RESTORED_EXCEPTION("Inflight data restore exception"),

	/* Unrecognized exception. */
	UNKNOWN_EXCEPTION("Unknown exception");

	private final String message;

	RestoreFailureReason(String message) {
		this.message = message;
	}

	public String getMessage() {
		return message;
	}
}
