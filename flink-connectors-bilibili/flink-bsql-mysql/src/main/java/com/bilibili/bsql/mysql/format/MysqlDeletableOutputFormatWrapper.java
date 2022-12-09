package com.bilibili.bsql.mysql.format;

import com.bilibili.bsql.mysql.sharding.ShardingMysqlOutputFormat;
import com.bilibili.bsql.mysql.tableinfo.MysqlSinkTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/** wraps MysqlOutputFormat and converts it to the primitive jdbc dynamic sink function */
public class MysqlDeletableOutputFormatWrapper {

    private final MysqlOutputFormat innerOutputFormat;

    private final JdbcBatchingOutputFormat<RowData, ?, ?> jdbcBatchingOutputFormat;

    public MysqlDeletableOutputFormatWrapper(
            DynamicTableSink.Context context,
            MysqlSinkTableInfo sinkTableInfo,
            MysqlOutputFormat innerOutputFormat) {
        this.innerOutputFormat = innerOutputFormat;
        this.jdbcBatchingOutputFormat =
                getJdbcDynamicOutputFormatBuilder(context, sinkTableInfo).build();
    }

    private JdbcDynamicOutputFormatBuilder getJdbcDynamicOutputFormatBuilder(
            DynamicTableSink.Context context, MysqlSinkTableInfo sinkTableInfo) {
        final TypeInformation<RowData> rowDataTypeInformation =
                (TypeInformation<RowData>)
                        context.createTypeInformation(sinkTableInfo.getRowDataType());
        final JdbcDynamicOutputFormatBuilder builder = new JdbcDynamicOutputFormatBuilder();

        builder.setJdbcOptions(sinkTableInfo.getJdbcOptions());
        builder.setJdbcDmlOptions(sinkTableInfo.getJdbcDmlOptions());
        builder.setJdbcExecutionOptions(sinkTableInfo.getJdbcExecutionOptions());
        builder.setRowDataTypeInfo(rowDataTypeInformation);
        builder.setFieldDataTypes(sinkTableInfo.getFieldDataTypes());
        builder.setJdbcConnectionProvider(new MysqlJdbcConnectionProvider(this.innerOutputFormat));

        return builder;
    }

    public SinkFunction<RowData> getJdbcBatchingSinkFunction() {
        return new OutputFormatSinkFunction(this.jdbcBatchingOutputFormat);
    }

	/**
	 * delegates connection provider into the jdbc real sink function,
	 * which is needed in some conditions like {@link ShardingMysqlOutputFormat}.
	 */
    static class MysqlJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

        private final MysqlOutputFormat mysqlOutputFormat;

        public MysqlJdbcConnectionProvider(MysqlOutputFormat mysqlOutputFormat) {
            this.mysqlOutputFormat = mysqlOutputFormat;
        }

        @Override
        public Connection getConnection() throws SQLException, ClassNotFoundException {
            return this.mysqlOutputFormat.establishConnection();
        }

        @Override
        public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
            return this.mysqlOutputFormat.reestablishConnection();
        }
    }
}
