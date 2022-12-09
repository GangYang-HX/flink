package com.bilibili.bsql.mysql.tableinfo;

import com.bilibili.bsql.mysql.table.BsqlMysqlDynamicSink;
import com.bilibili.bsql.rdb.tableinfo.RdbSinkTableInfo;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import static com.bilibili.bsql.mysql.tableinfo.MysqlConfig.BSQL_SINK_COMPRESS;
import static com.bilibili.bsql.mysql.tableinfo.MysqlConfig.BSQL_SINK_DELETE;

public class MysqlSinkTableInfo extends RdbSinkTableInfo {

    private final boolean enableCompress;

    private final boolean enableDelete;

    private JdbcOptions jdbcOptions;

    private JdbcExecutionOptions jdbcExecutionOptions;

    private JdbcDmlOptions jdbcDmlOptions;

    private DataType rowDataType;

    private DataType[] fieldDataTypes;

    private transient TableSchema tableSchema;

    public MysqlSinkTableInfo(
            FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        this.enableCompress = helper.getOptions().get(BSQL_SINK_COMPRESS);
        this.enableDelete = helper.getOptions().get(BSQL_SINK_DELETE);
        if (this.enableDelete) {
            initJdbcInfo(helper.getOptions(), context);
        }
    }

    private void initJdbcInfo(ReadableConfig config, DynamicTableFactory.Context context) {
        this.jdbcOptions = initJdbcOptions();
        this.jdbcExecutionOptions = initJdbcExecutionOptions(config);
        this.tableSchema = getTableSchema(context);
        this.jdbcDmlOptions = initJdbcDmlOptions(this.jdbcOptions, this.tableSchema);
        this.rowDataType = this.tableSchema.toRowDataType();
        this.fieldDataTypes = this.tableSchema.getFieldDataTypes();
    }

    public boolean isEnableCompress() {
        return enableCompress;
    }

    public boolean isEnableDelete() {
        return enableDelete;
    }

    public JdbcOptions getJdbcOptions() {
        return jdbcOptions;
    }

    public DataType getRowDataType() {
        return rowDataType;
    }

    public DataType[] getFieldDataTypes() {
        return fieldDataTypes;
    }

    public JdbcExecutionOptions getJdbcExecutionOptions() {
        return jdbcExecutionOptions;
    }

    public JdbcDmlOptions getJdbcDmlOptions() {
        return jdbcDmlOptions;
    }

    private JdbcOptions initJdbcOptions() {
        final JdbcOptions.Builder builder =
                JdbcOptions.builder()
                        .setDBUrl(this.getDbURL())
                        .setTableName(this.getTableName())
                        .setDialect(JdbcDialects.get(this.getDbURL()).get());

        builder.setDriverName(BsqlMysqlDynamicSink.MYSQL_DRIVER);
        builder.setUsername(this.getUserName());
        builder.setPassword(this.getPassword());
        return builder.build();
    }

    private JdbcExecutionOptions initJdbcExecutionOptions(ReadableConfig config) {
        final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
        builder.withBatchSize(getBatchMaxCount());
        builder.withBatchIntervalMs(getBatchMaxTimeout());
        builder.withMaxRetries(getMaxRetries());
        return builder.build();
    }

    private JdbcDmlOptions initJdbcDmlOptions(JdbcOptions jdbcOptions, TableSchema schema) {
        String[] keyFields =
                schema.getPrimaryKey()
                        .map(pk -> pk.getColumns().toArray(new String[0]))
                        .orElse(null);

        return JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(schema.getFieldNames())
                .withKeyFields(keyFields)
                .build();
    }

    private TableSchema getTableSchema(DynamicTableFactory.Context context) {
        return TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
    }
}
