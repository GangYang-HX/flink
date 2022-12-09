package com.bilibili.bsql.mysql.function;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.common.api.function.RdbRowDataLookupFunction;
import com.bilibili.bsql.mysql.client.SqlClient;
import com.bilibili.bsql.mysql.table.BsqlMysqlLookupOptionsEntity;
import io.vertx.ext.sql.SQLClient;

/** MysqlLookupFunction. */
@Internal
public class MysqlLookupFunction extends RdbRowDataLookupFunction {

    private BsqlMysqlLookupOptionsEntity options;

    public MysqlLookupFunction(
            BsqlMysqlLookupOptionsEntity options,
            LookupTableSource.LookupContext context,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        super(options, context, fieldNames, fieldTypes, keyNames, rowType);
        this.options = options;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        SQLClient sqlClient = SqlClient.getInstance(this.options);
        setRdbSQLClient(sqlClient);
    }
}
