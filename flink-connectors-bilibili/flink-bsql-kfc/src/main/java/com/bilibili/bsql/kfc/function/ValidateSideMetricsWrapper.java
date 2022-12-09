package com.bilibili.bsql.kfc.function;

import com.bilibili.bsql.common.metrics.SideMetricsWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xushaungshaung
 * @description
 * @date 2022/2/22
 **/
public class ValidateSideMetricsWrapper extends SideMetricsWrapper {

	private final ThreadLocal<MetricGroup> validationMetrics;
	private static final String TABLE = "kfc_table";

	private static final Map<String, ThreadLocal<Counter>> map = new HashMap();


	public ValidateSideMetricsWrapper(String tableName, FunctionContext runtimeContext) {
		super(tableName, runtimeContext);
		this.validationMetrics = ThreadLocal.withInitial(() -> globalGroup.addGroup("thread_" + sideTableName,
			Thread.currentThread().getName()));
	}

	public synchronized void validateMetric(String name, String prefix, int c) {
		if (!map.containsKey(name+prefix)){
			ThreadLocal<Counter> threadLocal = ThreadLocal.withInitial(() -> validationMetrics.get().addGroup(TABLE, prefix).counter(name));
			map.put(name+prefix, threadLocal);
		}
		map.get(name+prefix).get().inc(c);

	}

}
