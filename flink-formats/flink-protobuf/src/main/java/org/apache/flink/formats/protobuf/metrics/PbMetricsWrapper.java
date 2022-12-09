package org.apache.flink.formats.protobuf.metrics;

import org.apache.flink.metrics.Counter;

/**
 * @author: zhuzhengjun
 * @date: 2021/12/20 5:18 下午
 */
public class PbMetricsWrapper implements PbMetrics {
	Counter dirtyData;
	Counter defaultValue;


	public PbMetricsWrapper setDirtyData(Counter dirtyData) {
		this.dirtyData = dirtyData;
		return this;
	}

	public PbMetricsWrapper setDefaultValue(Counter defaultValue) {
		this.defaultValue = defaultValue;
		return this;
	}

	@Override
	public void dirtyData() {
		dirtyData.inc();
	}

	@Override
	public void defaultValue() {
		defaultValue.inc();
	}
}
