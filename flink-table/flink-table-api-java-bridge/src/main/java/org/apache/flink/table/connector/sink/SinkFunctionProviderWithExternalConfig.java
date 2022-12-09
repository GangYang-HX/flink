/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.table.connector.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

/**
 *
 * @author zhouxiaogang
 * @version $Id: SinkFunctionProviderWithShardingConfig.java, v 0.1 2021-01-12 14:02
zhouxiaogang Exp $$
 */
public interface SinkFunctionProviderWithExternalConfig extends SinkFunctionProviderWithParallel {

	static SinkFunctionProviderWithExternalConfig of(SinkFunction<RowData> sinkFunction, Integer parallelism, Integer shardNumber) {
		return new SinkFunctionProviderWithExternalConfig() {
			@Override
			public SinkFunction<RowData> createSinkFunction() {
				return sinkFunction;
			}

			@Override
			public Integer getConfiguredParallel() {
				return parallelism;
			}

			@Override
			public Integer getShardNumber(){
				return shardNumber;
			}
		};
	}

	/*
	 * Get the TP parallel
	 * */
	Integer getShardNumber();
}
