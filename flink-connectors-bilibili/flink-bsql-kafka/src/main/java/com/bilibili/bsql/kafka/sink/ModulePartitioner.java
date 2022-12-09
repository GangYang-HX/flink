/**
 * Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved.
 */

package com.bilibili.bsql.kafka.sink;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import static com.bilibili.bsql.common.utils.BinaryTransform.decodeBigEndian;

/**
 * @author zhouxiaogang
 * @version $Id: ModulePartitioner.java, v 0.1 2020-03-11 19:23 zhouxiaogang Exp $$
 */
public class ModulePartitioner extends FlinkKafkaPartitioner<RowData> {

    public final static String PARTITION_STRATEGY = "HASH";

    @Override
    public int partition(RowData record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(partitions != null && partitions.length > 0,
                                    "Partitions of the target topic is empty.");
        Preconditions.checkArgument(key != null && key.length == 4, "Partitions of the target topic is empty.");
        int hashValue = decodeBigEndian(key);

        return partitions[Math.abs(hashValue % partitions.length)];
    }

	@Override
	public String partitionerIdentifier() {
		return PARTITION_STRATEGY;
	}
}
