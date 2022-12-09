/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.hbase.table;


import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import com.bilibili.bsql.hbase.sink.BsqlHbaseDynamicSink;
import com.bilibili.bsql.hbase.tableinfo.HbaseSinkTableInfo;

import static com.bilibili.bsql.hbase.tableinfo.HbaseSinkTableInfo.*;


/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlHbaseDynamicSinkFactory.java, v 0.1 2020-10-13 16:52
zhouxiaogang Exp $$
 */
public class BsqlHbaseDynamicSinkFactory extends BsqlDynamicTableSinkFactory<HbaseSinkTableInfo> {

	public static final String IDENTIFIER = "bsql-hbase";

	@Override
	public HbaseSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context){
		return new HbaseSinkTableInfo(helper, context);
	}

	@Override
	public DynamicTableSink generateTableSink(HbaseSinkTableInfo tableInfo) {
		return new BsqlHbaseDynamicSink(tableInfo);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> basicOption) {
		basicOption.add(TABLE_NAME);
		basicOption.add(ZOOKEEPER_QUORUM);
		basicOption.add(ZOOKEEPER_ZNODE_PARENT);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> basicOption) {
		basicOption.add(VERSION);
		basicOption.add(SKIP_WAL);
	}
}
