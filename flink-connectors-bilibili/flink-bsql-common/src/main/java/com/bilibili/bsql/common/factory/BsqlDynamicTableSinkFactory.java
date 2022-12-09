/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common.factory;


import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.SinkTableInfo;

import static com.bilibili.bsql.common.keys.TableInfoKeys.*;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlDynamicTableSinkFactory.java, v 0.1 2020-10-13 14:27
zhouxiaogang Exp $$
 */
public abstract class BsqlDynamicTableSinkFactory<T extends SinkTableInfo> implements DynamicTableSinkFactory {
	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		return generateTableSink(parseContext(helper, context));
	}

	public abstract T parseContext(FactoryUtil.TableFactoryHelper helper, Context context);

	public abstract DynamicTableSink generateTableSink(T sinkTableInfo);


	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(BSQL_PARALLELISM);
		setRequiredOptions(options);
		return options;
	}

	public abstract void setRequiredOptions(Set<ConfigOption<?>> option);

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(BSQL_BATCH_TIMEOUT);
		options.add(BSQL_BATCH_SIZE);
		options.add(SABER_JOB_ID);
		setOptionalOptions(options);
		return options;
	}

	public abstract void setOptionalOptions(Set<ConfigOption<?>> option);
}
