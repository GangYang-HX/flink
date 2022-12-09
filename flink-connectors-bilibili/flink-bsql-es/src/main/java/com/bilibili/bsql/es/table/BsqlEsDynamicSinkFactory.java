package com.bilibili.bsql.es.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import com.bilibili.bsql.es.tableinfo.EsSinkTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.es.tableinfo.EsConfig.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlEsDynamicSinkFactory.java
 * @description This is the description of BsqlEsDynamicSinkFactory.java
 * @createTime 2020-10-28 11:54:00
 */
public class BsqlEsDynamicSinkFactory extends BsqlDynamicTableSinkFactory<EsSinkTableInfo> {
	public static final String IDENTIFIER = "bsql-es";

	@Override
	public EsSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
		return new EsSinkTableInfo(helper, context);
	}

	@Override
	public DynamicTableSink generateTableSink(EsSinkTableInfo sinkTableInfo) {
		return new BsqlEsDynamicSink(sinkTableInfo);
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_ES_ADDRESS);
		option.add(BSQL_ES_INDEX_NAME);
		option.add(BSQL_ES_TYPE_NAME);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> option) {

	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
