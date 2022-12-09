/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.latency.rockscache;


import org.apache.flink.api.common.state.MapState;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author zhouxiaogang
 * @version $Id: RocksCache.java, v 0.1 2021-02-04 21:16
zhouxiaogang Exp $$
 */
public class RocksCache  {
	private final static Logger LOG = LoggerFactory.getLogger(RocksCache.class);

	public transient MapState<String, CacheEntry> recordCache;

	protected Metric metric;

	public RocksCache(MapState recordCache, MetricGroup metricGroup) {
		this.recordCache = recordCache;
		if (metricGroup != null) {
			metric = new Metric(metricGroup.addGroup("joinCache"));
		}
	}

	public Boolean exist(String key) {
		boolean v = false;
		try {
			v = recordCache.contains(key);
		} catch (Exception e) {
			LOG.error("do exist error", e);
			metric.operationErrorCount.inc();
		}
		return v;
	}

	public CacheEntry get(String key) {
		CacheEntry v = null;
		try {
			v = recordCache.get(key);
		} catch (Exception e) {
			LOG.error("do get error", e);
			metric.operationErrorCount.inc();
		}
		return v;
	}

	public void delete(String key) {
		try {
			recordCache.remove(key);
		} catch (Exception e) {
			LOG.error("do delete error", e);
			metric.operationErrorCount.inc();
		}
	}

	public void put(String key, CacheEntry row) {
		try {
			recordCache.put(key, row);
		} catch (Exception e) {
			LOG.error("put data error", e);
			metric.operationErrorCount.inc();
		}
	}
}
