package org.apache.flink.bilibili.udf.aggregate.slidingwindowsum;

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
public class SlidingWindowSum extends AggregateFunction<Long, SlidingSumAccumulator> {

    private final static Logger LOG = LoggerFactory.getLogger(SlidingSumAccumulator.class);

    @Override
    public SlidingSumAccumulator createAccumulator() {
        return new SlidingSumAccumulator();
    }

    @Override
    public Long getValue(SlidingSumAccumulator acc) {
        if (acc == null) {
            LOG.error("acc is null");
            return 0L;
        }
        return acc.getValue();
    }

    public void accumulate(SlidingSumAccumulator acc, int addNumber, Long dataTime, Long standardMills,
                           Integer slidingMs, Integer windowSizeMs) {

        acc.add(addNumber, dataTime, slidingMs, windowSizeMs, standardMills);

    }

    @Override
    public TypeInformation<Long> getResultType() {
        return BasicTypeInfo.LONG_TYPE_INFO;
    }
}
