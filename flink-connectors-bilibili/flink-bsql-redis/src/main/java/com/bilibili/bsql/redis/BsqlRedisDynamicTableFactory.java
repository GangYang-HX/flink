/** Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved. */
package com.bilibili.bsql.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.bilibili.bsql.common.table.BsqlTableCommonOptions.BSQL_CACHE;
import static com.bilibili.bsql.common.table.BsqlTableCommonOptions.BSQL_CACHE_SIZE;
import static com.bilibili.bsql.common.table.BsqlTableCommonOptions.BSQL_CACHE_TTL;
import static com.bilibili.bsql.common.table.BsqlTableCommonOptions.BSQL_MULTIKEY_DELIMITKEY;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_PSWD;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_TABLE_NAME;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_UNAME;
import static com.bilibili.bsql.common.table.RdbConnectorOptions.BSQL_URL;
import static com.bilibili.bsql.redis.tableinfo.BsqlRedisConnectorOptions.BSQL_QUERY_TIME_OUT;
import static com.bilibili.bsql.redis.tableinfo.BsqlRedisConnectorOptions.BSQL_REDIS_TYPE;
import static com.bilibili.bsql.redis.tableinfo.BsqlRedisConnectorOptions.BSQL_RETRY_MAX_NUM;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.util.Preconditions.checkArgument;

/** @version BsqlRedisDynamicTableFactory.java */
public class BsqlRedisDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "bsql-redis";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        BsqlRedisLookupOptionsEntity bsqlRedisLookupOptions =
                getBsqlRedisLookupOptions(context, tableOptions);
        return new BsqlRedisDynamicTableSource(
                bsqlRedisLookupOptions, context.getPhysicalRowDataType());
    }

    private BsqlRedisLookupOptionsEntity getBsqlRedisLookupOptions(
            Context context, ReadableConfig readableConfig) {
        List<String> primaryKeys;
        Optional<UniqueConstraint> primaryConstraint =
                context.getCatalogTable().getResolvedSchema().getPrimaryKey();
        if (primaryConstraint.isPresent()) {
            primaryKeys = primaryConstraint.get().getColumns();
        } else {
            primaryKeys = new ArrayList<>();
        }
        List<Column> rawFields = context.getCatalogTable().getResolvedSchema().getColumns();
        checkArgument(rawFields.size() == 2, "redis has only k/v for now");

        List<Integer> primaryKeyIdxList =
                primaryKeys.stream()
                        .map(x -> fieldIndex(rawFields, x))
                        .collect(Collectors.toList());
        checkArgument(primaryKeyIdxList.size() == 1, "redis only support for one key");
        Integer primaryKeyIdx = primaryKeyIdxList.get(0);
        Integer valueIndex = 1 - primaryKeyIdx;
        String delimitKey = readableConfig.get(BSQL_MULTIKEY_DELIMITKEY);
        List<Column> physicalFields = getPhysicalFields(rawFields);
        boolean multiKey = false;
        if (delimitKey != null) {
            checkArgument(
                    physicalFields.get(valueIndex).getDataType().getLogicalType().getTypeRoot()
                            == ARRAY);
            multiKey = true;
        }

        BsqlRedisLookupOptionsEntity.Builder builder =
                BsqlRedisLookupOptionsEntity.newBuilder()
                        .setTableName(readableConfig.get(BSQL_TABLE_NAME))
                        .setTableType("redis")
                        .setRedisType(readableConfig.get(BSQL_REDIS_TYPE))
                        .setCacheSize(readableConfig.get(BSQL_CACHE_SIZE))
                        .setCacheTimeout(readableConfig.get(BSQL_CACHE_TTL))
                        .setCacheType(readableConfig.get(BSQL_CACHE))
                        .setMultikeyDelimiter(delimitKey)
                        .setUrl(readableConfig.get(BSQL_URL))
                        .setUsername(readableConfig.get(BSQL_UNAME))
                        .setPassword(readableConfig.get(BSQL_PSWD))
                        .setKeyIndex(primaryKeyIdx)
                        .setMultiKey(multiKey)
                        .setRetryMaxNum(readableConfig.get(BSQL_RETRY_MAX_NUM))
                        .setQueryTimeOut(readableConfig.get(BSQL_QUERY_TIME_OUT));
        return builder.build();
    }

    public List<Column> getPhysicalFields(List<Column> rawFields) {
        return rawFields.stream().filter(Column::isPhysical).collect(Collectors.toList());
    }

    public int fieldIndex(List<Column> rawFields, String fieldName) {
        for (int i = 0; i < rawFields.size(); i++) {
            if (fieldName.equals(rawFields.get(i).getName())) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BSQL_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BSQL_PSWD);
        options.add(BSQL_REDIS_TYPE);
        return options;
    }
}
