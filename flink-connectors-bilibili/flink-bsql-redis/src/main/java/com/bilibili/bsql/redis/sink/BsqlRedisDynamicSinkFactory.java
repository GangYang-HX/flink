/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis.sink;


import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;

import com.bilibili.bsql.redis.sink.BsqlRedisDynamicSink;
import com.bilibili.bsql.redis.tableinfo.RedisSinkTableInfo;

import static com.bilibili.bsql.redis.tableinfo.RedisSinkTableInfo.*;


/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlRedisDynamicSinkFactory.java, v 0.1 2020-10-13 16:52
zhouxiaogang Exp $$
 */
public class BsqlRedisDynamicSinkFactory extends BsqlDynamicTableSinkFactory<RedisSinkTableInfo> {

	public static final String IDENTIFIER = "bsql-redis";

	@Override
	public RedisSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context){

		return new RedisSinkTableInfo(helper, context);
	}

	@Override
	public DynamicTableSink generateTableSink(RedisSinkTableInfo tableInfo) {
		return new BsqlRedisDynamicSink(tableInfo);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> basicOption) {
		basicOption.add(REDIS_URL_KEY);
		basicOption.add(REDIS_TYPE_KEY);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> basicOption) {
		basicOption.add(REDIS_TIMEOUT);
		basicOption.add(MAXTOTAL_KEY);
		basicOption.add(MAXIDLE_KEY);
		basicOption.add(MINIDLE_KEY);
		basicOption.add(EXPIRE_KEY);
	}
}
