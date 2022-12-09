package com.bilibili.bsql.kafka.partitioner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.partitioner.BlacklistPartitioner;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * retry partitioner.
 *
 * @param <T>
 */
public abstract class RandomRetryPartitioner<T> extends BlacklistPartitioner<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RandomRetryPartitioner.class);

    protected int parallelInstanceId;
    protected int parallelInstances;
    private ConcurrentHashMap<Integer, Long> failedPart;
    private boolean kafkaFailRetry;
    protected transient ScheduledThreadPoolExecutor resetPool;

    public RandomRetryPartitioner() {}

    public void addFailedPartition(int partitionId) {
        this.failedPart.put(partitionId, System.currentTimeMillis());
    }

    public void resetPartition() {
        Long currentTime = System.currentTimeMillis();
        Iterator<Map.Entry<Integer, Long>> iterator = this.failedPart.entrySet().iterator();
        while (iterator.hasNext()) {
            Long partitionFailedTime = iterator.next().getValue();
            if ((currentTime - partitionFailedTime) > 10 * 60000L) {
                iterator.remove();
            }
        }
    }

    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(
                parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                parallelInstances > 0, "Number of subtasks must be larger than 0.");
        this.parallelInstanceId = parallelInstanceId;
        this.parallelInstances = parallelInstances;
        this.failedPart = new ConcurrentHashMap<>();
        if (this.kafkaFailRetry) {
            ThreadFactory namedThreadFactory =
                    new ThreadFactoryBuilder()
                            .setNameFormat("reset-failed-part-" + this.parallelInstanceId + "-%d")
                            .build();
            resetPool = new ScheduledThreadPoolExecutor(1, namedThreadFactory);
            resetPool.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            resetPartition();
                        }
                    },
                    1,
                    1,
                    TimeUnit.MINUTES);
        }
    }

    private synchronized void releaseFailedPartition(int[] partitions, String targetTopic) {
        if (judgeReleaseFailedPartition(partitions, targetTopic)) {
            Long earliestTime = Long.MAX_VALUE;
            Integer earliestPart = 0;
            for (Map.Entry<Integer, Long> s : this.failedPart.entrySet()) {
                if (s.getValue() < earliestTime) {
                    earliestTime = s.getValue();
                    earliestPart = s.getKey();
                }
            }
            this.failedPart.remove(earliestPart);
            LOG.info(
                    "kafka retry ID {} remove the part {} from black list, total part {}, failed {}",
                    parallelInstanceId,
                    earliestPart,
                    partitions.length,
                    this.failedPart.size());
        }
    }

    public boolean judgeReleaseFailedPartition(int[] partitions, String targetTopic) {
        if (!super.enableBlacklist()) {
            return partitions.length < failedPart.size() * 2;
        } else {
            List<String> blacklistTps =
                    super.allBlacklistTps()
                            .getOrDefault(
                                    new Tuple2<>(super.zkHost(), targetTopic),
                                    Collections.emptyList());
            return partitions.length < (failedPart.size() * 2 + blacklistTps.size());
        }
    }

    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        if (super.enableBlacklist()) {
            partitions = super.getFilteredPartitions(targetTopic, partitions);
        }
        if (kafkaFailRetry) {
            Preconditions.checkArgument(
                    partitions != null && partitions.length > 0,
                    "Partitions of the target topic is empty.");
            if (partitions.length < failedPart.size() * 2) {
                releaseFailedPartition(partitions, targetTopic);
            }
            int failPartSize = failedPart.size();
            if (failPartSize == 0) {
                return getPartition(record, key, value, targetTopic, partitions);
            } else {
                int startPartId = getPartition(record, key, value, targetTopic, partitions);
                while (failedPart.containsKey(partitions[startPartId % partitions.length])) {
                    startPartId = startPartId + randomNumber(failPartSize);
                }
                return partitions[startPartId % partitions.length];
            }
        } else {
            return getPartition(record, key, value, targetTopic, partitions);
        }
    }

    public abstract int getPartition(
            T record, byte[] key, byte[] value, String targetTopic, int[] partitions);

    private int randomNumber(int partitionLen) {
        partitionLen++;
        return ThreadLocalRandom.current().nextInt(1, partitionLen);
    }

    public boolean isKafkaFailRetry() {
        return kafkaFailRetry;
    }

    public void setKafkaFailRetry(boolean kafkaFailRetry) {
        this.kafkaFailRetry = kafkaFailRetry;
    }
}
