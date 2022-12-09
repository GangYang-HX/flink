package org.apache.flink.bili.writer.metricsetter;

import org.apache.flink.metrics.Counter;

/**
 * @author: zhuzhengjun
 * @date: 2021/8/11 8:51 下午
 */
public class TriggerMetricsWrapper implements TriggerMetrics {
	Counter failBackWatermark;

	private final Object lock = new Object();

	public TriggerMetricsWrapper setFailBackWatermark(Counter failBackWatermark) {
		this.failBackWatermark = failBackWatermark;
		return this;
	}

	@Override
	public void failBackWatermark() {
		synchronized (lock) {
			failBackWatermark.inc();
		}

	}
}
