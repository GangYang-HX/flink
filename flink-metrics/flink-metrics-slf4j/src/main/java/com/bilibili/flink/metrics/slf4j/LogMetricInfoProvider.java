package com.bilibili.flink.metrics.slf4j;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricGroup;

/** LogMetricInfoProvider. */
public interface LogMetricInfoProvider {
    LogMeasurementInfo getMetricInfo(
            String metricName,
            MetricGroup group,
            AbstractLogReporter.Filter labelFilter,
            CharacterFilter labelValueCharactersFilter);
}
