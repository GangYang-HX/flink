package com.bilibili.bsql.mysql.table;

import com.bilibili.bsql.mysql.function.MysqlLookupFunction;
import com.bilibili.bsql.mysql.tableinfo.MysqlSideTableInfo;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlMysqlDynamicSideSource.java
 * @description This is the description of BsqlMysqlDynamicSideSource.java
 * @createTime 2020-10-26 16:53:00
 */
public class BsqlMysqlDynamicSideSource implements LookupTableSource {
	private final MysqlSideTableInfo sideTableInfo;

	public BsqlMysqlDynamicSideSource(MysqlSideTableInfo sideTableInfo) {
		this.sideTableInfo = sideTableInfo;
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		return AsyncTableFunctionProviderWithParallel.of(
			new MysqlLookupFunction(sideTableInfo, context),
			sideTableInfo.getParallelism()
		);
	}

	@Override
	public DynamicTableSource copy() {
		return new BsqlMysqlDynamicSideSource(sideTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Mysql-Side-" + sideTableInfo.getName();
	}
}
