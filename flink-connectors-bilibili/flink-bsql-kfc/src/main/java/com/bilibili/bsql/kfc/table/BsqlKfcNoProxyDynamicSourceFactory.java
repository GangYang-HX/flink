/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kfc.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.kfc.tableinfo.KfcSideTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.kfc.tableinfo.KfcSideTableInfo.SERVER_URL_KEY;


/**
 *
 * @author jie
 * @version $Id: BsqlKfcCDDynamicSourceFactory.java, v 0.1 2022-02-21 16:52
jie Exp $$
 */
public class BsqlKfcNoProxyDynamicSourceFactory extends BsqlDynamicTableSideFactory<KfcSideTableInfo> {

	public static final String IDENTIFIER = "bsql-kfc-noproxy";

	@Override
	public KfcSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context){
		return new KfcSideTableInfo(helper, context);
	}

	@Override
	public DynamicTableSource generateTableSource(KfcSideTableInfo kafkaSinkTableInfo) {
		return new BsqlKfcNoProxyDynamicSource(kafkaSinkTableInfo);
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
