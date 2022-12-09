package com.bilibili.bsql.mysql.function;

import com.bilibili.bsql.mysql.factory.MysqlConnectFactory;
import com.bilibili.bsql.mysql.tableinfo.MysqlSideTableInfo;
import com.bilibili.bsql.rdb.function.RdbRowDataLookupFunction;
import com.bilibili.bsql.rdb.tableinfo.RdbSideTableInfo;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.functions.FunctionContext;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className MysqlLookupFunction.java
 * @description This is the description of MysqlLookupFunction.java
 * @createTime 2020-10-26 16:55:00
 */
public class MysqlLookupFunction extends RdbRowDataLookupFunction {
	private final MysqlSideTableInfo sideTableInfo;
	public final int[][] keys;

	public MysqlLookupFunction(RdbSideTableInfo sideTableInfo, LookupTableSource.LookupContext context) {
		super(sideTableInfo, context);
		this.sideTableInfo = (MysqlSideTableInfo) sideTableInfo;
		this.keys = context.getKeys();
	}

	@Override
	public void open0(FunctionContext context) throws Exception {
		int connectionPoolSize = sideTableInfo.getTmConnectPoolSize();
		SQLClient sqlClient = MysqlConnectFactory.getSqlClient(sideTableInfo, connectionPoolSize);
		setRdbSQLClient(sqlClient);
	}
}
