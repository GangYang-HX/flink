package com.bilibili.bsql.kafka.partitioner;

/**
 * https://info.bilibili.co/pages/viewpage.action?pageId=372778948 For more information please look
 * at wiki.
 */
public class BucketPartitioner<T> extends RandomRetryPartitioner<T> {

    public static final String PARTITION_STRATEGY = "BUCKET";

    private int bucketId;

    private int minBucketId;

    private int maxBucketId;

    private int partitionNum;

    @Override
    public int getPartition(
            T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        if (partitionNum != partitions.length) {
            partitionNum = partitions.length;
            minBucketId = parallelInstanceId == 0 ? 0 : parallelInstanceId * partitionNum;
            maxBucketId = minBucketId + (partitionNum - 1);
            bucketId = minBucketId;
        }
        int partition = bucketId / parallelInstances;
        if (bucketId < maxBucketId) {
            bucketId++;
        } else {
            bucketId = minBucketId;
        }
        return partition;
    }

    @Override
    public String partitionerIdentifier() {
        return PARTITION_STRATEGY;
    }
}
