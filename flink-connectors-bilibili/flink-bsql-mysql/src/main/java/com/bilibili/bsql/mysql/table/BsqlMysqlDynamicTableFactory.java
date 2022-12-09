package com.bilibili.bsql.mysql.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.table.RdbConnectorOptions;
import com.bilibili.bsql.common.utils.OptionUtils;

import java.util.HashSet;
import java.util.Set;

import static com.bilibili.bsql.common.table.BsqlTableCommonOptions.BSQL_CACHE;
import static com.bilibili.bsql.common.table.BsqlTableCommonOptions.BSQL_CACHE_SIZE;
import static com.bilibili.bsql.common.table.BsqlTableCommonOptions.BSQL_CACHE_TTL;
import static com.bilibili.bsql.common.table.BsqlTableCommonOptions.BSQL_MULTIKEY_DELIMITKEY;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_CONNECTION_POOL_SIZE;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_DRIVER_NAME;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_PSWD;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_TABLE_NAME;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_UNAME;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_URL;
import static com.bilibili.bsql.mysql.table.BsqlMysqlConnectorOptions.BSQL_IDLE_CONNECTION_TEST_PERIOD_TYPE;
import static com.bilibili.bsql.mysql.table.BsqlMysqlConnectorOptions.BSQL_MAX_IDLE_TIME;
import static com.bilibili.bsql.mysql.table.BsqlMysqlConnectorOptions.BSQL_TM_CONNECTION_POOL_SIZE;

/** BsqlMysqlDynamicTableFactory. */
public class BsqlMysqlDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "bsql-mysql";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        BsqlMysqlLookupOptionsEntity lookupOptions = getBsqlMysqlLookupOptions(tableOptions);
        OptionUtils.checkTableConfig(tableOptions);
        return new BsqlMysqlDynamicTableSource(lookupOptions, context.getPhysicalRowDataType());
    }

    private BsqlMysqlLookupOptionsEntity getBsqlMysqlLookupOptions(ReadableConfig readableConfig) {
        BsqlMysqlLookupOptionsEntity.Builder builder =
                BsqlMysqlLookupOptionsEntity.newBuilder()
                        .tableName(readableConfig.get(BSQL_TABLE_NAME))
                        .tableType("mysql")
                        .cacheSize(readableConfig.get(BSQL_CACHE_SIZE))
                        .cacheTimeout(readableConfig.get(BSQL_CACHE_TTL))
                        .cacheType(readableConfig.get(BSQL_CACHE))
                        .multikeyDelimiter(readableConfig.get(BSQL_MULTIKEY_DELIMITKEY))
                        .url(readableConfig.get(BSQL_URL))
                        .password(readableConfig.get(BSQL_PSWD))
                        .username(readableConfig.get(BSQL_UNAME))
                        .connectionPoolSize(readableConfig.get(BSQL_CONNECTION_POOL_SIZE))
                        .diverName(readableConfig.get(BSQL_DRIVER_NAME))
                        .idleConnectionTestPeriodType(
                                readableConfig.get(BSQL_IDLE_CONNECTION_TEST_PERIOD_TYPE))
                        .maxIdleTime(readableConfig.get(BSQL_MAX_IDLE_TIME))
                        .tmConnectionPoolSize(readableConfig.get(BSQL_TM_CONNECTION_POOL_SIZE));
        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RdbConnectorOptions.BSQL_URL);
        options.add(RdbConnectorOptions.BSQL_UNAME);
        options.add(RdbConnectorOptions.BSQL_PSWD);
        options.add(RdbConnectorOptions.BSQL_TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }
}
