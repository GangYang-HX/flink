package org.apache.flink.bilibili.udf.aggregate.windowsumtop;


import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangyang
 * @Date:2020/5/6
 * @Time:4:01 PM
 */
public class WindowSumTop extends AggregateFunction<String, SumTopAccumulator> {

    private final static Logger LOG = LoggerFactory.getLogger(WindowSumTop.class);

    @Override
    public SumTopAccumulator createAccumulator() {
        return new SumTopAccumulator();
    }

    @Override
    public String getValue(SumTopAccumulator accumulator) {
        return accumulator.getValue();
    }

    public void accumulate(SumTopAccumulator acc, Integer topN, String subKey, Long addNumber, Long eventTime,
                           Long standardTime, Integer windowSizeMs) {

        acc.add(topN, subKey, addNumber, eventTime, windowSizeMs, standardTime);

    }

    @Override
    public TypeInformation<String> getResultType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    @Override
    public TypeInformation<SumTopAccumulator> getAccumulatorType() {
        return TypeInformation.of(SumTopAccumulator.class);
    }
}
