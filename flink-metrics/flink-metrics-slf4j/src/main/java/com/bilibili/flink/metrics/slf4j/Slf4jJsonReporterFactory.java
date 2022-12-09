package com.bilibili.flink.metrics.slf4j;

import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Properties;

/** Slf4jJsonReporterFactory. */
@InterceptInstantiationViaReflection(
        reporterClassName = "com.bilibili.flink.metrics.slf4j.Slf4jJsonReporter")
public class Slf4jJsonReporterFactory implements MetricReporterFactory {

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new Slf4jJsonReporter();
    }
}
