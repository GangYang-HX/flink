/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis;

import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.redis.tableinfo.RedisSideTableInfo;

import static com.bilibili.bsql.redis.tableinfo.RedisConfig.*;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlRedisDynamicTableFactory.java, v 0.1 2020-09-30 16:32
zhouxiaogang Exp $$
 */
public class BsqlRedisDynamicTableFactory extends BsqlDynamicTableSideFactory<RedisSideTableInfo> {

	public static final String IDENTIFIER = "bsql-redis";

	@Override
	public RedisSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context){
		return new RedisSideTableInfo(helper, context);
	}

	@Override
	public DynamicTableSource generateTableSource(RedisSideTableInfo sourceTableInfo) {
		return new BsqlRedisDynamicSource(sourceTableInfo);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> basicOption) {
		/**
		* url is necessary
		* */
		basicOption.add(BSQL_REDIS_URL);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> basicOption) {
		basicOption.add(BSQL_REDIS_PSWD);
		basicOption.add(BSQL_REDIS_TYPE);
	}
}
