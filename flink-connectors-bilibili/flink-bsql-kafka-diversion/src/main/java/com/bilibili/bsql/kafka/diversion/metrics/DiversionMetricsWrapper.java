package com.bilibili.bsql.kafka.diversion.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.QueryServiceMode;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.bilibili.bsql.common.metrics.Metrics.WINDOW_SIZE;
import static com.bilibili.bsql.kafka.diversion.metrics.DiversionMetricsConstant.DIVERSION_PRODUCER;
import static com.bilibili.bsql.kafka.diversion.metrics.DiversionMetricsConstant.DIVERSION_TOPIC;
import static com.bilibili.bsql.kafka.diversion.metrics.DiversionMetricsConstant.WRITE_SUCCESS_RECORD;
import static com.bilibili.bsql.kafka.diversion.metrics.DiversionMetricsConstant.RT_WRITE_RECORD;

public class DiversionMetricsWrapper implements DiversionMetricService {

	protected final MetricGroup metricGroup;
	protected final Object diversionLock = new Object();
	private final Map<String, Map<String, Counter>> successWriteCounter = new ConcurrentHashMap<>(8, 0.75f, 2);
	private final Map<String, Map<String, Histogram>> writeRtHistogram = new ConcurrentHashMap<>(8, 0.75f, 2);

	public DiversionMetricsWrapper(MetricGroup metricGroup) {
		this.metricGroup = metricGroup;
	}

	public void writeSuccess(String broker, String topic) {
		Counter counter = getMetrics(broker, topic, successWriteCounter, s -> metricGroup.addGroup(DIVERSION_PRODUCER,
			broker, QueryServiceMode.DISABLED)
			.addGroup(DIVERSION_TOPIC, topic, QueryServiceMode.DISABLED)
			.counter(WRITE_SUCCESS_RECORD));
		synchronized (diversionLock) {
			counter.inc();
		}
	}

	@Override
	public void writeFailed(String broker, String topic) {

	}

	public void rtWrite(String broker, String topic, long start) {
		Histogram histogram = getMetrics(broker, topic, writeRtHistogram, s -> metricGroup.addGroup(DIVERSION_PRODUCER,
			broker, QueryServiceMode.DISABLED)
			.addGroup(DIVERSION_TOPIC, topic, QueryServiceMode.DISABLED)
			.histogram(RT_WRITE_RECORD, new DescriptiveStatisticsHistogram(WINDOW_SIZE)));
		synchronized (diversionLock) {
			histogram.update(System.nanoTime() - start);
		}
	}

	@Override
	public void retryCount(String broker, String topic) {

	}

	protected <V> V getMetrics(String broker, String topic, Map<String, Map<String, V>> map, Function<String, V> registFunction) {
		Map<String, V> topicMap = map.computeIfAbsent(broker, s -> new HashMap<>());
		return topicMap.computeIfAbsent(topic, registFunction);
	}

}
