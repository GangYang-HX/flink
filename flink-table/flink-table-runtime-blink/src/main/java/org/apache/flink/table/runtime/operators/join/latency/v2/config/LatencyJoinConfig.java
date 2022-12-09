package org.apache.flink.table.runtime.operators.join.latency.v2.config;

import java.io.Serializable;

/**
 * Author: wangding01
 * Date: 2022/2/24
 * Time: 18:26
 */
public class LatencyJoinConfig implements Serializable {

	private boolean joinMergeEnable;
	private String joinMergeDelimiter;
	private boolean distinctJoin;
	private boolean globalJoin;
	private boolean ttlEnable;
	private boolean keyedStateTtlEnable = false;
	private long cleanUpInterval;

	public String getJoinMergeDelimiter() {
		return joinMergeDelimiter;
	}

	public void setJoinMergeDelimiter(String joinMergeDelimiter) {
		this.joinMergeDelimiter = joinMergeDelimiter;
	}

	public boolean isDistinctJoin() {
		return distinctJoin;
	}

	public void setDistinctJoin(boolean distinctJoin) {
		this.distinctJoin = distinctJoin;
	}

	public void setGlobalJoin(boolean globalJoin) {
		this.globalJoin = globalJoin;
	}

	public boolean isGlobalJoin() {
		return globalJoin;
	}

	public boolean isTtlEnable() {
		return ttlEnable;
	}

	public void setTtlEnable(boolean ttlEnable) {
		this.ttlEnable = ttlEnable;
	}

    public boolean isJoinMergeEnable() {
        return joinMergeEnable;
    }

    public void setJoinMergeEnable(boolean joinMergeEnable) {
        this.joinMergeEnable = joinMergeEnable;
    }

	public void setKeyedStateTtlEnable(boolean keyedStateTtlEnable) {
		this.keyedStateTtlEnable = keyedStateTtlEnable;
	}

	public boolean isKeyedStateTtlEnable() {
		return this.keyedStateTtlEnable;
	}

	public long getCleanUpInterval() {
		return cleanUpInterval;
	}

	public void setCleanUpInterval(long cleanUpInterval) {
		this.cleanUpInterval = cleanUpInterval;
	}
}
