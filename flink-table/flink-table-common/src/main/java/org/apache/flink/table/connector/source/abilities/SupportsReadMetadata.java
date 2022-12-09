package org.apache.flink.table.connector.source.abilities;

import java.util.List;

public interface SupportsReadMetadata {
	void applyReadMetadata(List<String> metadataKeys, int[] metadataProjectIndices);
}
