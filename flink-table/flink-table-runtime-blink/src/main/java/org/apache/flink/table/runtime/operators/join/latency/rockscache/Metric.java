package org.apache.flink.table.runtime.operators.join.latency.rockscache;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.table.runtime.operators.join.latency.DefaultMetricsConfig;

public class Metric {

	public Counter put;
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
	public Counter operationErrorCount;

	public Metric(MetricGroup metricGroup) {
		operationErrorCount = metricGroup.counter("error");
		put = metricGroup.counter("put");
		putRT = metricGroup.histogram("putRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
		get = metricGroup.counter("get");
		getRT = metricGroup.histogram("getRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
		scan = metricGroup.counter("scan");
		scanRT = metricGroup.histogram("scanRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
		del = metricGroup.counter("del");
		delRT = metricGroup.histogram("delRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
		exist = metricGroup.counter("exist");
		existRT = metricGroup.histogram("existRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
		bucket = metricGroup.counter("bucket");
		bucketRT = metricGroup.histogram("bucketRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));
	}
}
