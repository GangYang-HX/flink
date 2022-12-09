/** Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved. */
package com.bilibili.bsql.redis;

import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.redis.source.RedisLookupFunction;

/**
 * @version 2022/11/16
 * @see LookupTableSource
 */
public class BsqlRedisDynamicTableSource implements LookupTableSource {

    public BsqlRedisLookupOptionsEntity lookupOptions;
    private DataType physicalRowDataType;

    public BsqlRedisDynamicTableSource(
            BsqlRedisLookupOptionsEntity lookupOptions, DataType physicalRowDataType) {
        this.lookupOptions = lookupOptions;
        this.physicalRowDataType = physicalRowDataType;
    }

    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProvider.of(
                new RedisLookupFunction(
                        this.lookupOptions,
                        context,
                        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        (RowType) physicalRowDataType.getLogicalType()));
    }

    @Override
    public BsqlRedisDynamicTableSource copy() {
        return new BsqlRedisDynamicTableSource(this.lookupOptions, this.physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "Redis table source";
    }
}
