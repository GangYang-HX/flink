package org.apache.flink.bili.writer.metricsetter;

/**
 * @author: zhuzhengjun
 * @date: 2021/8/11 8:49 下午
 */
public interface TriggerMetrics extends Metrics {

	/**
	 * fail back watermark count
	 */
	void failBackWatermark();
}
