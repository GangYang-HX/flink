/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.table.connector.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;

/**
 *
 * @author zhouxiaogang
 * @version $Id: SourceFunctionProviderWithParallel.java, v 0.1 2020-09-29 14:14
zhouxiaogang Exp $$
 */
public interface SourceFunctionProviderWithParallel extends SourceFunctionProvider {

	/**
	 * Helper method for creating a static provider.
	 */
	static SourceFunctionProviderWithParallel of(
		SourceFunction<RowData> sourceFunction,
		boolean isBounded,
		Integer parallelism) {
		return new SourceFunctionProviderWithParallel() {
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
		};
	}

	/**
	 * Get the Parallel to enable config.
	 */
	Integer getConfiguredParallel();
}
