package com.bilibili.bsql.vbase.table;

import com.bilibili.bsql.vbase.function.BsqlVbaseLookupFunction;
import com.bilibili.bsql.vbase.tableinfo.VbaseSideTableInfo;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

/**
 * @author zhuzhengjun
 * @date 2020/11/4 11:14 上午
 */
public class BsqlVbaseDynamicSideSource implements LookupTableSource {

    private final VbaseSideTableInfo vbaseSideTableInfo;

    public BsqlVbaseDynamicSideSource(VbaseSideTableInfo vbaseSideTableInfo) {
        this.vbaseSideTableInfo = vbaseSideTableInfo;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return AsyncTableFunctionProviderWithParallel.of(
        	new BsqlVbaseLookupFunction(vbaseSideTableInfo, lookupContext),
			vbaseSideTableInfo.getParallelism()
		);
    }

    @Override
    public DynamicTableSource copy() {
        return new BsqlVbaseDynamicSideSource(vbaseSideTableInfo);
    }

    @Override
    public String asSummaryString() {
        return "Vbase-Side-" + vbaseSideTableInfo.getName();
    }

}
