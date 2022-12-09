package org.apache.flink.bilibili.udf.aggregate.windowtop;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyang
 * @Date:2020/4/20
 * @Time:3:40 PM
 */
public class WindowTop extends AggregateFunction<String, TopAccumulator> {

    private final static Logger LOG = LoggerFactory.getLogger(WindowTop.class);

    @Override
    public TopAccumulator createAccumulator() {
        return new TopAccumulator();
    }

    @Override
    public String getValue(TopAccumulator accumulator) {
        return accumulator.getValue();
    }

    public void accumulate(TopAccumulator acc, Integer topN, String subKey, Long sortKey, Long eventTime,
                           Long standardTime, Integer windowSizeMs) {
        acc.add(topN, subKey, sortKey, eventTime, windowSizeMs, standardTime);


    }

    @Override
    public TypeInformation<String> getResultType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    @Override
    public TypeInformation<TopAccumulator> getAccumulatorType() {
        return TypeInformation.of(TopAccumulator.class);
    }
}
