package com.bilibili.flink.metrics.slf4j;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricGroup;

/**
 * @author Dove
 * @Date 2021/8/25 9:58 下午
 */
public interface LogMetricInfoProvider {
	LogMeasurementInfo getMetricInfo(String metricName, MetricGroup group, AbstractLogReporter.Filter labelFilter, CharacterFilter labelValueCharactersFilter);
}
