package com.bilibili.bsql.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * @author Shin
 * @date 2021/9/10 17:30
 */

public class KafkaSinkDistributedTP {
	private static final Random random = new Random();
	private final static Logger LOG = LoggerFactory.getLogger(KafkaSinkDistributedTP.class);

	public static int getPartition(int[] partitions, int parallelNumber, int parallelInstanceId) {
		int PartitionLength = partitions.length;
		if (parallelNumber >= PartitionLength) {
			int PartitionNumber = partitions[parallelInstanceId % PartitionLength];
			LOG.debug("use hash partition,to :{}", PartitionNumber);
			return PartitionNumber;
		} else {
			int PartitionNumber = partitions[calculate(parallelInstanceId, parallelNumber, PartitionLength)];
			LOG.debug("use group partition, to:{}", PartitionNumber);
			return PartitionNumber;
		}
	}
	private static int calculate(int parallelInstanceId, int parallelNumber, int partitionLength) {
		int divisor = partitionLength / parallelNumber;
		int remainder = partitionLength % parallelNumber;
		if (parallelInstanceId < parallelNumber - remainder) {
			return random.nextInt(divisor) + parallelInstanceId * divisor;
		} else {
			return random.nextInt(divisor + 1) + parallelInstanceId * divisor + remainder - (parallelNumber - parallelInstanceId);
		}
	}
}
