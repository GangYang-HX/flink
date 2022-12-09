package com.bilibili.bsql.kfc.table;

import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.LookupTableSource;

import com.bilibili.bsql.kfc.function.KfcRowDataLookupFunction;
import com.bilibili.bsql.kfc.tableinfo.KfcSideTableInfo;

public class BsqlKfcDynamicSource implements LookupTableSource {

	public KfcSideTableInfo sideTableInfo;

	public BsqlKfcDynamicSource(KfcSideTableInfo sideTableInfo) {
		this.sideTableInfo = sideTableInfo;
	}

	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		return AsyncTableFunctionProviderWithParallel.of(
			new KfcRowDataLookupFunction(sideTableInfo, context),
			sideTableInfo.getParallelism()
		);
	}

	@Override
	public BsqlKfcDynamicSource copy() {
		return new BsqlKfcDynamicSource(sideTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Kfc-Side-" + sideTableInfo.getName();
	}
}
