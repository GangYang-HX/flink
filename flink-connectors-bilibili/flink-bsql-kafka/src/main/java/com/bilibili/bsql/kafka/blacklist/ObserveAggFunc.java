package com.bilibili.bsql.kafka.blacklist;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * @Author weizefeng
 * @Date 2022/2/26 20:27
 **/
public class ObserveAggFunc implements AggregateFunction<Properties, ObserveLagAccumulator, Properties>, Serializable {

    public final static String NAME = ObserveAggFunc.class.getSimpleName();

    private final transient static Logger LOG = LoggerFactory.getLogger(ObserveAggFunc.class);

    @Override
    public ObserveLagAccumulator createAccumulator() {
        LOG.info("Thread {} create accumulator success", Thread.currentThread().getName());
        return new ObserveLagAccumulator();
    }

    @Override
    public ObserveLagAccumulator add(Properties observeInput, ObserveLagAccumulator accumulator) {
        accumulator.update(observeInput);
        return accumulator;
    }

    @Override
    public Properties getResult(ObserveLagAccumulator accumulator) {
        Properties properties = new Properties();
        properties.put("allBlacklistTps", accumulator.getAllBlacklistTp());
        return properties;
    }

    @Override
    public ObserveLagAccumulator merge(ObserveLagAccumulator a, ObserveLagAccumulator b) {
        return null;
    }
}
