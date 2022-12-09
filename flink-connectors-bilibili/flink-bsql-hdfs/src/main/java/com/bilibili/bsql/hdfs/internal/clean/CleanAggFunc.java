package com.bilibili.bsql.hdfs.internal.clean;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyang
 * @Date:2020/7/23
 * @Time:2:59 PM
 */
public class CleanAggFunc implements AggregateFunction<CleanInput, CleanAccumulator, CleanOutput> {

    public static final String NAME = CleanAggFunc.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(CleanAggFunc.class);

    @Override
    public CleanAccumulator createAccumulator() {
        LOG.info("createAccumulator success");
        return new CleanAccumulator();
    }

    @Override
    public CleanAccumulator add(CleanInput cleanInput, CleanAccumulator cleanAccumulator) {
        cleanAccumulator.update(cleanInput);
        return cleanAccumulator;
    }

    @Override
    public CleanOutput getResult(CleanAccumulator cleanAccumulator) {
        return new CleanOutput(cleanAccumulator.getResult());
    }

    @Override
    public CleanAccumulator merge(CleanAccumulator cleanAccumulator, CleanAccumulator acc1) {
        return cleanAccumulator.merge(acc1);
    }
}
