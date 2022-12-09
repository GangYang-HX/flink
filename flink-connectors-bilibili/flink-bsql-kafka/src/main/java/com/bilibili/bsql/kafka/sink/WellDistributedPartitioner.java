package com.bilibili.bsql.kafka.sink;

import com.bilibili.bsql.kafka.util.KafkaSinkDistributedTP;
import org.apache.flink.streaming.connectors.kafka.partitioner.RandomRetryPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Shin
 * @date 2021/8/26 16:31
 */

public class WellDistributedPartitioner<T> extends RandomRetryPartitioner<T> {
	public final static String PARTITION_STRATEGY = "DISTRIBUTE";
	private final static Logger LOG = LoggerFactory.getLogger(WellDistributedPartitioner.class);

	@Override
	public int getPartition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
		return KafkaSinkDistributedTP.getPartition(partitions,this.parallelInstances,this.parallelInstanceId);
	}

	@Override
	public String partitionerIdentifier() {
		return PARTITION_STRATEGY;
	}
}
