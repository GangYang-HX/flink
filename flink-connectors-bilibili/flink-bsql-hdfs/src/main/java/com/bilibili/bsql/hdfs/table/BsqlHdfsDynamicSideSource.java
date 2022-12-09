package com.bilibili.bsql.hdfs.table;

import com.bilibili.bsql.hdfs.function.HdfsLookupFunction;
import com.bilibili.bsql.hdfs.tableinfo.HdfsSideTableInfo;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlHdfsDynamicSideSource.java
 * @description This is the description of BsqlHdfsDynamicSideSource.java
 * @createTime 2020-10-22 21:42:00
 */
public class BsqlHdfsDynamicSideSource implements LookupTableSource {
	private final HdfsSideTableInfo sideTableInfo;

	public BsqlHdfsDynamicSideSource(HdfsSideTableInfo sideTableInfo) {
		this.sideTableInfo = sideTableInfo;
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		return AsyncTableFunctionProviderWithParallel.of(
			new HdfsLookupFunction(sideTableInfo, context),
			sideTableInfo.getParallelism()
		);
	}

	@Override
	public DynamicTableSource copy() {
		return new BsqlHdfsDynamicSideSource(sideTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Hdfs-Side-" + sideTableInfo.getName();
	}
}
