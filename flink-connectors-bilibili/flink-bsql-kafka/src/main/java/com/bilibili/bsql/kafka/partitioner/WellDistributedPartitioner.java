package com.bilibili.bsql.kafka.partitioner;

import com.bilibili.bsql.kafka.util.KafkaSinkDistributedTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Support partition dynamic discovery. */
public class WellDistributedPartitioner<T> extends RandomRetryPartitioner<T> {
    public static final String PARTITION_STRATEGY = "DISTRIBUTE";
    private static final Logger LOG = LoggerFactory.getLogger(WellDistributedPartitioner.class);

    @Override
    public int getPartition(
            T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return KafkaSinkDistributedTP.getPartition(
                partitions, this.parallelInstances, this.parallelInstanceId);
    }

    @Override
    public String partitionerIdentifier() {
        return PARTITION_STRATEGY;
    }
}
