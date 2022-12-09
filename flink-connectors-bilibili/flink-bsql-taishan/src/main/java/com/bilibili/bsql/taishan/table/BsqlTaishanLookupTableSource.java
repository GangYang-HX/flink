package com.bilibili.bsql.taishan.table;

import com.bilibili.bsql.taishan.lookup.TaishanLookupFunction;
import com.bilibili.bsql.taishan.tableinfo.TaishanSideTableInfo;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

public class BsqlTaishanLookupTableSource implements LookupTableSource {

    private final TaishanSideTableInfo taishanSideTableInfo;

    public BsqlTaishanLookupTableSource(TaishanSideTableInfo taishanSideTableInfo) {
        this.taishanSideTableInfo = taishanSideTableInfo;
    }

    @Override
    public DynamicTableSource copy() {
        return new BsqlTaishanLookupTableSource(taishanSideTableInfo);
    }

    @Override
    public String asSummaryString() {
        return "Taishan-Side-" + taishanSideTableInfo.getName();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProviderWithParallel.of(
                new TaishanLookupFunction(taishanSideTableInfo, context),
                taishanSideTableInfo.getParallelism()
        );
    }
}
