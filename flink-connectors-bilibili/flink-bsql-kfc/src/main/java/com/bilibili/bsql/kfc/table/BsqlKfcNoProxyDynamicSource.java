package com.bilibili.bsql.kfc.table;

import com.bilibili.bsql.kfc.function.KfcNoProxyRowDataLookupFunction;
import com.bilibili.bsql.kfc.tableinfo.KfcSideTableInfo;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.LookupTableSource;

public class BsqlKfcNoProxyDynamicSource implements LookupTableSource {

	public KfcSideTableInfo sideTableInfo;

	public BsqlKfcNoProxyDynamicSource(KfcSideTableInfo sideTableInfo) {
		this.sideTableInfo = sideTableInfo;
	}

	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		return AsyncTableFunctionProviderWithParallel.of(
			new KfcNoProxyRowDataLookupFunction(sideTableInfo, context),
			sideTableInfo.getParallelism()
		);
	}

	@Override
	public BsqlKfcNoProxyDynamicSource copy() {
		return new BsqlKfcNoProxyDynamicSource(sideTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Kfc-Side-" + sideTableInfo.getName();
	}
}
