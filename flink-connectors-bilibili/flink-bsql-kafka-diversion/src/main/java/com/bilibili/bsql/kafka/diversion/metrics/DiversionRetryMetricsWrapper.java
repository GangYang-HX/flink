package com.bilibili.bsql.kafka.diversion.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.QueryServiceMode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.bilibili.bsql.kafka.diversion.metrics.DiversionMetricsConstant.*;

public class DiversionRetryMetricsWrapper extends DiversionMetricsWrapper {
	private final Map<String, Map<String, Counter>> failureWriteCounter = new ConcurrentHashMap<>(8, 0.75f, 2);
	private final Map<String, Map<String, Counter>> retryCounter = new ConcurrentHashMap<>(8, 0.75f, 2);

	public DiversionRetryMetricsWrapper(MetricGroup metricGroup) {
		super(metricGroup);
	}

	@Override
	public void writeFailed(String broker, String topic) {
		Counter counter = getMetrics(broker, topic, failureWriteCounter, s ->
			metricGroup.addGroup(DIVERSION_PRODUCER, broker, QueryServiceMode.DISABLED)
				.addGroup(DIVERSION_TOPIC, topic)
				.counter(WRITE_FAILURE_RECORD));
		synchronized (diversionLock) {
			counter.inc();
		}
	}

	@Override
	public void retryCount(String broker, String topic) {
		Counter counter = getMetrics(broker, topic, retryCounter, s ->
			metricGroup.addGroup(DIVERSION_PRODUCER, broker, QueryServiceMode.DISABLED)
				.addGroup(DIVERSION_TOPIC, topic)
				.counter(RETRY_RECORD));
		synchronized (diversionLock) {
			counter.inc();
		}
	}
}
