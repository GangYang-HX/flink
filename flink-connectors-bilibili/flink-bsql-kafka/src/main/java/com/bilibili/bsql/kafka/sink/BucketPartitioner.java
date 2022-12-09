/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.sink;

import org.apache.flink.streaming.connectors.kafka.partitioner.RandomRetryPartitioner;
import org.apache.flink.table.data.RowData;

/**
 * For more information please look at wiki https://info.bilibili.co/pages/viewpage.action?pageId=372778948
 * @author xiaoyu
 * @version $Id: BucketPartitioner.java, v 0.1 2021-12-22 17:38
 */
public class BucketPartitioner<T>  extends RandomRetryPartitioner<T> {

	public final static String PARTITION_STRATEGY = "BUCKET";

	private int bucketId;

	private int minBucketId;

	private int maxBucketId;

	private int partitionNum;

	@Override
	public int getPartition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
		if(partitionNum != partitions.length){
			partitionNum = partitions.length;
			minBucketId = parallelInstanceId == 0 ? 0 : parallelInstanceId * partitionNum;
			maxBucketId = minBucketId + (partitionNum - 1);
			bucketId = minBucketId;
		}
		int partition = bucketId/parallelInstances;
		if(bucketId < maxBucketId){
			bucketId++;
		}else {
			bucketId = minBucketId;
		}
		return partitions[partition];
	}

	@Override
	public String partitionerIdentifier() {
		return PARTITION_STRATEGY;
	}
}
