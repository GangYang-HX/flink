package org.apache.flink.api.common.state;

/**
 * A base interface for all backend batch writer.
 */
public interface BatchWriter {

	/**
	 * Push data into the pending data set, flush the batch writer if needed.
	 *
	 * @param stateName The state name.
	 * @param key  The raw key.
	 * @param value The raw value.
	 */
	void add(String stateName, byte[] key, byte[] value) throws Exception;

	/**
	 * Close the batch writer. This should also flush if there is any pending data.
	 *
	 */
	void closeBatchWriter() throws Exception;
}
