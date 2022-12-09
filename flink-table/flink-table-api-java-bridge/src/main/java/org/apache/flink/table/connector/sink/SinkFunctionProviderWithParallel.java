/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.table.connector.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

/**
 *
 * @author zhouxiaogang
 * @version $Id: SinkFunctionProviderWithParallel.java, v 0.1 2020-10-21 11:04
zhouxiaogang Exp $$
 */
public interface SinkFunctionProviderWithParallel extends SinkFunctionProvider {
	static SinkFunctionProviderWithParallel of(SinkFunction<RowData> sinkFunction, Integer parallelism) {
		return new SinkFunctionProviderWithParallel() {
			@Override
			public SinkFunction<RowData> createSinkFunction() {
				return sinkFunction;
			}

			@Override
			public Integer getConfiguredParallel() {
				return parallelism;
			}
		};
	}

	/**
	 * Get the Parallel to enable config.
	 */
	Integer getConfiguredParallel();
}


