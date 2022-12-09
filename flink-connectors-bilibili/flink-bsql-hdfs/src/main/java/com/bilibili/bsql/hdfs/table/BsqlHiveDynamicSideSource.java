package com.bilibili.bsql.hdfs.table;

import com.bilibili.bsql.hdfs.function.HiveLookupFunction;
import com.bilibili.bsql.hdfs.tableinfo.HiveSideTableInfo;
import org.apache.flink.table.connector.source.AsyncTableFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

/**
 * @Author: JinZhengyu
 * @Date: 2022/7/28 上午10:12
 */
public class BsqlHiveDynamicSideSource implements LookupTableSource {

    private final HiveSideTableInfo sideTableInfo;

    public BsqlHiveDynamicSideSource(HiveSideTableInfo sideTableInfo) {
        this.sideTableInfo = sideTableInfo;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProviderWithParallel.of(
                new HiveLookupFunction(sideTableInfo, context),
                sideTableInfo.getParallelism()
        );
    }

    @Override
    public DynamicTableSource copy() {
        return new BsqlHiveDynamicSideSource(sideTableInfo);
    }

    @Override
    public String asSummaryString() {
        return "Hive-Side-" + sideTableInfo.getName();
    }
}
