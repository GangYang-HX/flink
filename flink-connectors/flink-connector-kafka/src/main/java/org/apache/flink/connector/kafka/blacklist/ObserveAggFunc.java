package org.apache.flink.connector.kafka.blacklist;

import org.apache.flink.api.common.functions.AggregateFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/** ObserveAggFunc. */
public class ObserveAggFunc
        implements AggregateFunction<Properties, ObserveLagAccumulator, Properties>, Serializable {

    public static final String NAME = ObserveAggFunc.class.getSimpleName();

    private static final transient Logger LOG = LoggerFactory.getLogger(ObserveAggFunc.class);

    public static final String PROPS_KEY_ALL_BLACKLIST_TPS = "allBlacklistTps";

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
        properties.put(PROPS_KEY_ALL_BLACKLIST_TPS, accumulator.getAllBlacklistTp());
        return properties;
    }

    @Override
    public ObserveLagAccumulator merge(ObserveLagAccumulator a, ObserveLagAccumulator b) {
        return null;
    }
}
