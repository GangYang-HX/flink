package org.apache.flink.streaming.connectors.kafka.internals.metrics;


import org.apache.flink.metrics.Metric;

/**
 * @author: zhuzhengjun
 * @date: 2021/11/17 5:52 下午
 */
public interface KafkaConsumerMetrics extends Metric {

	/**
	 * kafka polls rt
	 * @param start time to start poll records
	 */
	void pollsTimeUse(long start, long end);

	void recordPollTimeMs(long pollStartMs, long pollEndMs);

	void recordPollRatio(long pollStart);

}
