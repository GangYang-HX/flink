/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bili.writer.sink.assigner;

import org.apache.flink.bili.writer.partition.PartitionComputer;
import org.apache.flink.bili.writer.partition.PartitionPathUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

/**
 * Table bucket assigner, wrap {@link PartitionComputer}.
 */
public class TableBucketAssigner<T> implements BucketAssigner<T, String> {

	private final PartitionComputer<T> computer;

	public TableBucketAssigner(PartitionComputer<T> computer) {
		this.computer = computer;
	}

	@Override
	public String getBucketId(T element, Context context) {
		try {
			return PartitionPathUtils.generatePartitionPath(
				computer.generatePartValues(element, context));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public SimpleVersionedSerializer<String> getSerializer() {
		return SimpleVersionedStringSerializer.INSTANCE;
	}
}
