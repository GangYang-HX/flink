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

import com.bilibili.bsql.common.SideTableInfo;

import static com.bilibili.bsql.common.keys.TableInfoKeys.*;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlDynamicTableSideFactory.java, v 0.1 2020-10-13 14:28
zhouxiaogang Exp $$
 */
public abstract class BsqlDynamicTableSideFactory<T extends SideTableInfo> implements DynamicTableSourceFactory {

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		return generateTableSource(parseContext(helper, context));
	}

	public abstract T parseContext(FactoryUtil.TableFactoryHelper helper, Context context);

	public abstract DynamicTableSource generateTableSource(T sourceTableInfo);

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		setRequiredOptions(options);
		return options;
	}

	public abstract void setRequiredOptions(Set<ConfigOption<?>> option);

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(BSQL_CACHE);
		options.add(BSQL_CACHE_SIZE);
		options.add(BSQL_CACHE_TTL);
		options.add(SABER_JOB_ID);
		options.add(BSQL_MULTIKEY_DELIMITKEY);
		setOptionalOptions(options);
		return options;
	}

	public abstract void setOptionalOptions(Set<ConfigOption<?>> option);

}
