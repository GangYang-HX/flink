package com.bilibili.bsql.rdb.table;

import com.bilibili.bsql.rdb.function.RdbRowDataLookupFunction;
import com.bilibili.bsql.rdb.tableinfo.RdbSideTableInfo;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlRdbDynamicSource.java
 * @description This is the description of BsqlRdbDynamicSource.java
 * @createTime 2020-10-20 14:19:00
 */
public class BsqlRdbDynamicSource implements LookupTableSource {

	public RdbSideTableInfo sideTableInfo;

	public BsqlRdbDynamicSource(RdbSideTableInfo sideTableInfo) {
		this.sideTableInfo = sideTableInfo;
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		return AsyncTableFunctionProviderWithParallel.of(
			new RdbRowDataLookupFunction(sideTableInfo, context),
			sideTableInfo.getParallelism()
		);
	}

	@Override
	public DynamicTableSource copy() {
		return new BsqlRdbDynamicSource(sideTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Rdb-Side-" + sideTableInfo.getName();
	}
}
