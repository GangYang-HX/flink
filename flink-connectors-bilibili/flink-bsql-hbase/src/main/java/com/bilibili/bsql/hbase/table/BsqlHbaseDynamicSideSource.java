package com.bilibili.bsql.hbase.table;

import com.bilibili.bsql.hbase.function.HbaseLookupFunction;
import com.bilibili.bsql.hbase.tableinfo.HbaseSideTableInfo;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

/**
 * @author zhuzhengjun
 * @date 2020/11/12 6:51 下午
 */
public class BsqlHbaseDynamicSideSource implements LookupTableSource {


    private final HbaseSideTableInfo hbaseSideTableInfo;

    public BsqlHbaseDynamicSideSource(HbaseSideTableInfo sideTableInfo) {
        this.hbaseSideTableInfo = sideTableInfo;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return AsyncTableFunctionProviderWithParallel.of(
        	new HbaseLookupFunction(hbaseSideTableInfo, lookupContext),
			hbaseSideTableInfo.getParallelism()
		);
    }

    @Override
    public DynamicTableSource copy() {
        return new BsqlHbaseDynamicSideSource(hbaseSideTableInfo);
    }

    @Override
    public String asSummaryString() {
        return "Hbase-Side-" + hbaseSideTableInfo.getName();
    }
}
