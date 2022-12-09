/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.latency.cache;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import static org.apache.flink.table.runtime.operators.join.latency.DefaultMetricsConfig.DEFAULT_WINDOW_SIZE;

/**
 * @author zhouhuidong
 * @version $Id: Metric.java, v 0.1 2021-07-07 9:46 下午 zhouhuidong Exp $$
 */
public class Metric {

	public Counter put;
	public Counter ttlPut;
	public Histogram putRT;
	public Counter get;
	public Histogram getRT;
	public Counter scan;
	public Histogram scanRT;
	public Counter del;
	public Histogram delRT;
	public Counter exist;
	public Histogram existRT;
	public Counter bucket;
	public Histogram bucketRT;

	private static Metric instance = null;

	public static Metric getInstance(MetricGroup metricGroup) {
		if (instance == null) {
			synchronized (Metric.class) {
				if (instance == null) {
					instance = new Metric(metricGroup);
				}
			}
		}
		return instance;
	}

	private Metric(MetricGroup metricGroup) {

		put = metricGroup.counter("put");
		ttlPut = metricGroup.counter("ttlPut");
		putRT = metricGroup.histogram("putRT", new DescriptiveStatisticsHistogram(DEFAULT_WINDOW_SIZE));
		get = metricGroup.counter("get");
		getRT = metricGroup.histogram("getRT", new DescriptiveStatisticsHistogram(DEFAULT_WINDOW_SIZE));
		scan = metricGroup.counter("scan");
		scanRT = metricGroup.histogram("scanRT", new DescriptiveStatisticsHistogram(DEFAULT_WINDOW_SIZE));
		del = metricGroup.counter("del");
		delRT = metricGroup.histogram("delRT", new DescriptiveStatisticsHistogram(DEFAULT_WINDOW_SIZE));
		exist = metricGroup.counter("exist");
		existRT = metricGroup.histogram("existRT", new DescriptiveStatisticsHistogram(DEFAULT_WINDOW_SIZE));
		bucket = metricGroup.counter("bucket");
		bucketRT = metricGroup.histogram("bucketRT", new DescriptiveStatisticsHistogram(DEFAULT_WINDOW_SIZE));
	}
}
