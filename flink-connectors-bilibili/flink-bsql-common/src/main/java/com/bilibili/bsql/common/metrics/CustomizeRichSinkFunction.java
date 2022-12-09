package com.bilibili.bsql.common.metrics;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.QueryServiceMode;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Sink table should implement this class to get its metric.
 */
public abstract class CustomizeRichSinkFunction<IN> extends RichSinkFunction<IN> implements Metrics {

    /**
     * Prefix to all sink metrics on grafana.
     */
    private static final String SINK_METRICS = "SinkMetrics";

    /**
     * Counter name to metric success write.
     */
    private static final String WRITE_SUCCESS_RECORD = "WriteSuccessRecord";

    /**
     * Counter name to metric failed write.
     */
    private static final String WRITE_FAILURE_RECORD = "WriteFailureRecord";

    /**
     * Histogram name to write rt.
     */
    private static final String RT_WRITE_RECORD = "RtWriteRecord";

	/**
	 * Histogram name to flush rt
	 */
	private static final String RT_FLUSH_RECORD = "RtFlushRt";

	/**
	 * Counter name to metric retry numbers
	 */
	private static final String RETRY_NUMBER_RECORD = "RetryNumberRecord";

    protected SinkMetricsWrapper sinkMetricsGroup;

    @Override
    public void open(Configuration parameters) throws Exception {
		String clazz = sinkType();
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(SINK_METRICS, clazz, QueryServiceMode.DISABLED);
		sinkMetricsGroup = new SinkMetricsWrapper()
			.setSuccessfulWrites(metricGroup.counter(WRITE_SUCCESS_RECORD))
			.setFailedWrites(metricGroup.counter(WRITE_FAILURE_RECORD))
			.setWriteRt(metricGroup.histogram(RT_WRITE_RECORD, new DescriptiveStatisticsHistogram(WINDOW_SIZE)))
			.setRetryNumber(metricGroup.counter(RETRY_NUMBER_RECORD))
			.setFlushRt(metricGroup.histogram(RT_FLUSH_RECORD, new DescriptiveStatisticsHistogram(WINDOW_SIZE)));
        doOpen(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
    }

    public void doOpen(int taskNumber, int numTasks) throws IOException {
        // overwrite this method if needed.
    }

    public String sinkType() {
        // overwrite this method if needed.
        return "";
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        long start = System.nanoTime();
        boolean writeSuccess = doInvoke(value, context);
        sinkMetricsGroup.rtWriteRecord(start);
        if (!writeSuccess) {
            sinkMetricsGroup.writeFailureRecord();
        }
        sinkMetricsGroup.writeSuccessRecord();
    }

    public abstract boolean doInvoke(IN value, Context context) throws Exception;

    @Override
    public void close() throws Exception {
        doClose();
        // to prevent memory leak
        sinkMetricsGroup = null;
    }

    public void doClose() throws Exception {
        // overwrite this method if needed.
    }
}
