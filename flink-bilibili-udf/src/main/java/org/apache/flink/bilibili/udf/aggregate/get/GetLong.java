package org.apache.flink.bilibili.udf.aggregate.get;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author zhangyang
 * @Date:2020/6/17
 * @Time:2:46 PM
 */
public class GetLong extends AggregateFunction<Long, SimpleAccumulator> {

    @Override
    public SimpleAccumulator createAccumulator() {
        return new SimpleAccumulator();
    }

    public void accumulate(SimpleAccumulator acc, Object val) {
        acc.set(val);
    }

    @Override
    public Long getValue(SimpleAccumulator simpleAccumulator) {
        return simpleAccumulator.get();
    }
}
