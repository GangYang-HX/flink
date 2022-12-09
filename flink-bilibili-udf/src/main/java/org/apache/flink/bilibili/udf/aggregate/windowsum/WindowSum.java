package org.apache.flink.bilibili.udf.aggregate.windowsum;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyang
 * @Date:2020/4/21
 * @Time:3:26 PM
 */
public class WindowSum extends AggregateFunction<Long, SumAccumulator> {

    private final static Logger LOG = LoggerFactory.getLogger(WindowSum.class);

    @Override
    public SumAccumulator createAccumulator() {
        SumAccumulator acc = new SumAccumulator();
        return acc;
    }

    @Override
    public Long getValue(SumAccumulator acc) {
        if (acc == null) {
            LOG.error("acc is null");
            return 0L;
        }
        return acc.getValue();
    }

    public void accumulate(SumAccumulator acc, long addNumber, Long dataTime, Long standardMills, Integer windowSizeMs) {
        acc.add(addNumber, dataTime, windowSizeMs, standardMills);

    }

    @Override
    public TypeInformation<Long> getResultType() {
        return BasicTypeInfo.LONG_TYPE_INFO;
    }
}
