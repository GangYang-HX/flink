package com.bilibili.bsql.clickhouse.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import com.bilibili.bsql.rdb.tableinfo.RdbSinkTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_BATCH_SIZE;
import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_PARALLELISM;
import static com.bilibili.bsql.rdb.tableinfo.RdbConfig.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlClickHouseDynamicSinkFactory.java
 * @description This is the description of BsqlClickHouseDynamicSinkFactory.java
 * @createTime 2020-10-21 14:38:00
 */
public class BsqlClickHouseDynamicSinkFactory extends BsqlDynamicTableSinkFactory<RdbSinkTableInfo> {

	public static final String IDENTIFIER = "bsql-clickhouse";

	@Override
	public RdbSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
		return new RdbSinkTableInfo(helper, context);
	}

	@Override
	public DynamicTableSink generateTableSink(RdbSinkTableInfo sinkTableInfo) {
		return new BsqlClickHouseDynamicSink(sinkTableInfo);
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_URL);
		option.add(BSQL_UNAME);
		option.add(BSQL_PSWD);
		option.add(BSQL_TABLE_NAME);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_TPS);
		option.add(BSQL_BATCH_SIZE);
		option.add(BSQL_PARALLELISM);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
