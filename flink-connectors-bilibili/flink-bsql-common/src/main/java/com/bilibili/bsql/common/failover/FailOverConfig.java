package com.bilibili.bsql.common.failover;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author: zhuzhengjun
 * @date: 2021/3/18 2:23 下午
 */
public class FailOverConfig {
	/**
	 * failOverRate
	 */
	private static final String FAILOVER_RATE = "failOverRate";
	/**
	 * statistics time interval
	 */
	private static final String FAILOVER_TIME_LENGTH = "failOverTimeLength";
	/**
	 * failOver time out value
	 */
	private static final String BASE_FAIL_BACK_INTERVAL = "baseFailBackInterval";
	/**
	 * failOver  Switch
	 */
	private static final String FAILOVER_SWITCH_ON = "failOverSwitchOn";
	/**
	 * failOver index max ratios
	 * calc : Sum(failedIndex)/Sum(allIndex)
	 */
	private static final String MAX_FAILED_INDEX_RATIO = "maxFailedIndexRatio";

	public static final ConfigOption<Double> BSQL_FAILOVER_RATE = ConfigOptions
		.key(FAILOVER_RATE)
		.doubleType()
		.defaultValue(0.1)
		.withDescription("bsql failOverRate");

	public static final ConfigOption<Integer> BSQL_FAILOVER_TIME_LENGTH = ConfigOptions
		.key(FAILOVER_TIME_LENGTH)
		.intType()
		.defaultValue(1)
		.withDescription("bsql failOverTimeLength");

	public static final ConfigOption<Integer> BSQL_BASE_FAIL_BACK_INTERVAL = ConfigOptions
		.key(BASE_FAIL_BACK_INTERVAL)
		.intType()
		.defaultValue(300000)
		.withDescription("bsql baseFailBackInterval");

	public static final ConfigOption<Boolean> BSQL_FAILOVER_SWITCH_ON = ConfigOptions
		.key(FAILOVER_SWITCH_ON)
		.booleanType()
		.defaultValue(true)
		.withDescription("bsql failOverSwitchOn");

	public static final ConfigOption<Double> BSQL_MAX_FAILED_INDEX_RATIO = ConfigOptions
		.key(MAX_FAILED_INDEX_RATIO)
		.doubleType()
		.defaultValue(0.5)
		.withDescription("bsql maxFailedIndexRatio");


}
