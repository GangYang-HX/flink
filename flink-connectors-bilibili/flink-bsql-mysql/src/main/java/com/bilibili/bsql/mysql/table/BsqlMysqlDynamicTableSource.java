package com.bilibili.bsql.mysql.table;

import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.mysql.function.MysqlLookupFunction;

/** BsqlMysqlDynamicTableSource. */
public class BsqlMysqlDynamicTableSource implements LookupTableSource {

    private BsqlMysqlLookupOptionsEntity lookupOptions;
    private DataType physicalRowDataType;

    public BsqlMysqlDynamicTableSource(
            BsqlMysqlLookupOptionsEntity lookupOptions, DataType physicalRowDataType) {
        this.lookupOptions = lookupOptions;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProvider.of(
                new MysqlLookupFunction(
                        this.lookupOptions,
                        context,
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                        new String[0],
                        (RowType) physicalRowDataType.getLogicalType()));
    }

    @Override
    public DynamicTableSource copy() {
        return new BsqlMysqlDynamicTableSource(this.lookupOptions, this.physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "Mysql table source";
    }
}
