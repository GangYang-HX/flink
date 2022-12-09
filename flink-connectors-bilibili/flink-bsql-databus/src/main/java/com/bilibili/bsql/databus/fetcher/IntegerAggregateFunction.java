package com.bilibili.bsql.databus.fetcher;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Created with IntelliJ IDEA.
 * 累加器
 *
 * @author weiximing
 * @version 1.0.0
 * @className IntegerAggregateFunction.java
 * @description This is the description of IntegerAggregateFunction.java
 * @createTime 2020-10-22 18:50:00
 */
public class IntegerAggregateFunction implements AggregateFunction<Integer, Integer, Integer> {
	private static final long serialVersionUID = 1718886437717369902L;

	@Override
	public Integer createAccumulator() {
		return 0;
	}

	@Override
	public Integer add(Integer value, Integer accumulator) {
		return value + accumulator;
	}

	@Override
	public Integer getResult(Integer accumulator) {
		return accumulator;
	}

	@Override
	public Integer merge(Integer accumulatorA, Integer accumulatorB) {
		return add(accumulatorA, accumulatorB);
	}
}
