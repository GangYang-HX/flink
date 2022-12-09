package com.bilibili.bsql.clickhouse.shard.failover;

import com.bilibili.bsql.common.failover.BatchOutFormatFailOver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class HashFailOver extends BatchOutFormatFailOver {

    private static final Logger LOG = LoggerFactory.getLogger(HashFailOver.class);
    private final List<BatchOutFormatFailOver> failOvers;
    private int tempShardIndex;

    public HashFailOver(double failOverRate, int failoverTimeLength, long baseFailBackInterval, boolean failOverSwitchOn,
                        int allIndex, double maxFailedIndexRatio) {
        super(failOverRate, failoverTimeLength, baseFailBackInterval, failOverSwitchOn, allIndex, maxFailedIndexRatio);
        failOvers = new ArrayList<>();
    }

    public static HashFailOver createUselessHashFailOver() {
        return new HashFailOver(0, 0, 0, true, 0, 0);
    }

    public void addFailover(BatchOutFormatFailOver failOver) {
        //should be deposited in order
        failOvers.add(failOver);
        LOG.info("add hash failOver, replica: {}", failOver.getAllIndex());
    }

    @Override
    public Integer selectSinkIndex(Integer shardIndex) {
        //attention: not thread safe
        tempShardIndex = shardIndex;
        BatchOutFormatFailOver failOver = failOvers.get(shardIndex);
        return failOver.selectSinkIndex(ThreadLocalRandom.current().nextInt(failOver.getAllIndex()));
    }

    @Override
    public void failedIndexRecord(Integer index, Boolean isSuccess) {
        //attention: not thread safe
        BatchOutFormatFailOver failOver = failOvers.get(tempShardIndex);
        failOver.failedIndexRecord(index, isSuccess);
    }
}
