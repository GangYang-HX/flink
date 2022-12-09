package com.bilibili.bsql.mysql.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import com.bilibili.bsql.mysql.tableinfo.MysqlSinkTableInfo;
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
 * @className BsqlMysqlDynamicSinkFactory.java
 * @description This is the description of BsqlMysqlDynamicSinkFactory.java
 * @createTime 2020-10-22 15:02:00
 */
public class BsqlMysqlDynamicSinkFactory extends BsqlDynamicTableSinkFactory<MysqlSinkTableInfo> {

	public static final String IDENTIFIER = "bsql-mysql";

	@Override
	public MysqlSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
		return new MysqlSinkTableInfo(helper, context);
	}

	@Override
	public DynamicTableSink generateTableSink(MysqlSinkTableInfo sinkTableInfo) {
		return new BsqlMysqlDynamicSink(sinkTableInfo);
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
