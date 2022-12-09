package com.bilibili.bsql.mysql.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSideFactory;
import com.bilibili.bsql.mysql.tableinfo.MysqlSideTableInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_BATCH_SIZE;
import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_PARALLELISM;
import static com.bilibili.bsql.rdb.tableinfo.RdbConfig.*;
import static com.bilibili.bsql.rdb.tableinfo.RdbConfig.BSQL_TABLE_NAME;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlMysqlDynamicSideFactory.java
 * @description This is the description of BsqlMysqlDynamicSideFactory.java
 * @createTime 2020-10-26 16:51:00
 */
public class BsqlMysqlDynamicSideFactory extends BsqlDynamicTableSideFactory<MysqlSideTableInfo> {

	public static final String IDENTIFIER = "bsql-mysql";

	@Override
	public MysqlSideTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
		return new MysqlSideTableInfo(helper, context);
	}

	@Override
	public DynamicTableSource generateTableSource(MysqlSideTableInfo sourceTableInfo) {
		return new BsqlMysqlDynamicSideSource(sourceTableInfo);
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
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
