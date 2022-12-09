package org.apache.flink.table.runtime.operators.join.interval.config;

import java.io.Serializable;

/**
 * author wangding01
 * Date: 2021/12/30
 * Time: 20:26
 */
public class IntervalJoinConfig implements Serializable {

	private long cleanUpInterval;
	private boolean keyedStateTtlEnable = false;

	public long getCleanUpInterval() {
		return cleanUpInterval;
	}

	public void setCleanUpInterval(long cleanUpInterval) {
		this.cleanUpInterval = cleanUpInterval;
	}

	public void setKeyedStateTtlEnable(boolean keyedStateTtlEnable) {
		this.keyedStateTtlEnable = keyedStateTtlEnable;
	}

	public boolean isKeyedStateTtlEnable() {
		return this.keyedStateTtlEnable;
	}
}
