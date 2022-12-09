/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common.factory;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.SourceTableInfo;

import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_PARALLELISM;
import static com.bilibili.bsql.common.keys.TableInfoKeys.SABER_JOB_ID;

/**
 * @author zhouxiaogang
 * @version $Id: BsqlDynamicTableSourceFactory.java, v 0.1 2020-10-13 14:27
 * zhouxiaogang Exp $$
 */
public abstract class BsqlDynamicTableSourceFactory<T extends SourceTableInfo> implements DynamicTableSourceFactory {
	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		return generateTableSource(parseContext(helper, context));
	}

	/**
	 * 用于生成TableInfo（基类）
	 * 类似saber中的KafkaSourceParser
	 *
	 * @param helper
	 * @param context
	 * @return
	 */
	public abstract T parseContext(FactoryUtil.TableFactoryHelper helper, Context context);

	/**
	 * 用于生成IStream...，真正的source
	 * 类似saber中的IStreamSourceGener->genStreamSource->table
	 *
	 * @param sourceTableInfo
	 * @return
	 */
	public abstract DynamicTableSource generateTableSource(T sourceTableInfo);

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
		options.add(SABER_JOB_ID);
		setOptionalOptions(options);
		return options;
	}

	public abstract void setOptionalOptions(Set<ConfigOption<?>> option);
}
