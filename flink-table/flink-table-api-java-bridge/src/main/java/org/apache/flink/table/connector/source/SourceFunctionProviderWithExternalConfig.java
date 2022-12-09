/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.table.connector.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;

/**
 *
 * @author zhouxiaogang
 * @version $Id: SourceFunctionProviderWithShardingConfig.java, v 0.1 2021-01-12 14:07
zhouxiaogang Exp $$
 */
public interface SourceFunctionProviderWithExternalConfig extends SourceFunctionProviderWithParallel {

	static SourceFunctionProviderWithExternalConfig of(
		SourceFunction<RowData> sourceFunction,
		boolean isBounded,
		Integer parallelism,
		Integer shardSize) {
		return new SourceFunctionProviderWithExternalConfig() {
			@Override
			public SourceFunction<RowData> createSourceFunction() {
				return sourceFunction;
			}

			@Override
			public boolean isBounded() {
				return isBounded;
			}

			@Override
			public Integer getConfiguredParallel(){
				return parallelism;
			}

			@Override
			public Integer getShardNumber(){
				return shardSize;
			}
		};
	}

	Integer getShardNumber();
}
