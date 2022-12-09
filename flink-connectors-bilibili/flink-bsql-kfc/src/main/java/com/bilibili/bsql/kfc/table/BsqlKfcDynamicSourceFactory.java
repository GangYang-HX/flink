/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kfc.table;

import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;


import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.kfc.tableinfo.KfcSideTableInfo;

import static com.bilibili.bsql.kfc.tableinfo.KfcSideTableInfo.SERVER_URL_KEY;


/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlKfcDynamicSourceFactory.java, v 0.1 2020-10-13 16:52
zhouxiaogang Exp $$
 */
public class BsqlKfcDynamicSourceFactory extends BsqlDynamicTableSideFactory<KfcSideTableInfo> {

	public static final String IDENTIFIER = "bsql-kfc";

	@Override
	public KfcSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context){
		return new KfcSideTableInfo(helper, context);
	}

	@Override
	public DynamicTableSource generateTableSource(KfcSideTableInfo kafkaSinkTableInfo) {
		return new BsqlKfcDynamicSource(kafkaSinkTableInfo);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> basicOption) {
		basicOption.add(SERVER_URL_KEY);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> basicOption) {

	}

}
