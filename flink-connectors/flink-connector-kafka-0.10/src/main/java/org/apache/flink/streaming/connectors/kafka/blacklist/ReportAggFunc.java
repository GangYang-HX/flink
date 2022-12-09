package org.apache.flink.streaming.connectors.kafka.blacklist;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Author weizefeng
 * @Date 2022/2/23 15:23
 **/
public class ReportAggFunc implements AggregateFunction<Properties, ReportLagAccumulator, Properties>, Serializable {

    public final static String NAME = ReportAggFunc.class.getSimpleName();

    private final transient static Logger LOG = LoggerFactory.getLogger(ReportAggFunc.class);

    private ZkReporter zkReporter = null;

    @Override
    public ReportLagAccumulator createAccumulator() {
        return new ReportLagAccumulator();
    }

    /**
     *
     * @param reportInput The value to add
     * @param accumulator The accumulator to add the value to
     *
     * @return
     */
    @Override
    public ReportLagAccumulator add(Properties reportInput, ReportLagAccumulator accumulator) {
        accumulator.update(reportInput);
        return accumulator;
    }

    @Override
    public Properties getResult(ReportLagAccumulator accumulator) {
        Properties properties = new Properties();
        return properties;
    }

    @Override
    public ReportLagAccumulator merge(ReportLagAccumulator a, ReportLagAccumulator b) {
        return null;
    }

}
