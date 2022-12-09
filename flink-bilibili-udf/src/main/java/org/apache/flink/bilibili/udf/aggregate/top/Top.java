package org.apache.flink.bilibili.udf.aggregate.top;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Top extends AggregateFunction<String, TopAccumulator> {

    private final static Logger LOG = LoggerFactory.getLogger(Top.class);

    @Override
    public TopAccumulator createAccumulator() {
        return new TopAccumulator();
    }

    @Override
    public String getValue(TopAccumulator accumulator) {
        return accumulator.getValue();
    }

    public void accumulate(TopAccumulator acc, Integer topN, String subDimension, Long subMetric) {
        acc.add(topN, subDimension, subMetric);
    }

    public void retract(TopAccumulator acc, Integer topN, String subDimension, Long subMetric) {
       acc.retract(topN, subDimension, subMetric);
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
