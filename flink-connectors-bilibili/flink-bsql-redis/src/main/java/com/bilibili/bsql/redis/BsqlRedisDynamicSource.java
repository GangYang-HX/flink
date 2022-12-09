/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis;

import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;

import com.bilibili.bsql.redis.source.RedisRowDataLookupFunction;
import com.bilibili.bsql.redis.tableinfo.RedisSideTableInfo;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlRedisDynamicSource.java, v 0.1 2020-10-07 16:22
zhouxiaogang Exp $$
 */
public class BsqlRedisDynamicSource implements LookupTableSource {

	public RedisSideTableInfo sideTableInfo;

	public BsqlRedisDynamicSource(RedisSideTableInfo sideTableInfo) {
		this.sideTableInfo = sideTableInfo;
	}

	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		return AsyncTableFunctionProviderWithParallel.of(
			new RedisRowDataLookupFunction(sideTableInfo, context),
			sideTableInfo.getParallelism()
		);
	}

	@Override
	public BsqlRedisDynamicSource copy() {
		return new BsqlRedisDynamicSource(sideTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Redis-Side-" + sideTableInfo.getName();
	}
}
