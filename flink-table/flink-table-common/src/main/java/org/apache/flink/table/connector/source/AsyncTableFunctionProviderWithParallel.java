/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.table.connector.source;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;

/**
 *
 * @author zhouxiaogang
 * @version $Id: AsyncTableFunctionProviderWithParallel.java, v 0.1 2020-12-08 15:28
zhouxiaogang Exp $$
 */
public interface AsyncTableFunctionProviderWithParallel<T> extends AsyncTableFunctionProvider<T>{

	static <T> AsyncTableFunctionProviderWithParallel<T> of(AsyncTableFunction<T> asyncTableFunction,
															Integer parallelism) {
		return new AsyncTableFunctionProviderWithParallel() {
			@Override
			public AsyncTableFunction<T> createAsyncTableFunction() {
				return asyncTableFunction;
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
