package org.apache.flink.table.connector.sink.abilities;

import java.util.List;

public interface SupportsWriteMetadata {
	void applyWriteMetadata(List<String> metadataKeys, int[] metadataProjectIndices);
}
