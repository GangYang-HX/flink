package com.bilibili.bsql.common.api.function;

import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.bilibili.bsql.common.cache.cacheobj.CacheContentType;
import com.bilibili.bsql.common.cache.cacheobj.CacheMissVal;
import com.bilibili.bsql.common.cache.cacheobj.CacheObj;
import com.bilibili.bsql.common.table.LookupOptionsEntityBase;
import com.bilibili.bsql.common.utils.LookupUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** class for Mysql, MongoDB ,Redis side table's side table LookUpFunction. */
public class RdbRowDataLookupFunction extends AsyncLookupFunctionBase {

    private static final Logger LOG = LoggerFactory.getLogger(RdbRowDataLookupFunction.class);

    private transient SQLClient rdbSQLClient;
    public int[][] keys;
    public static final int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;
    public static final int DEFAULT_VERTX_WORKER_POOL_SIZE = 10;

    private String sqlCondition;
    protected String[] fieldNames;
    protected DataType[] fieldTypes;

    public RdbRowDataLookupFunction(
            LookupOptionsEntityBase lookupOptions,
            LookupTableSource.LookupContext context,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        super(lookupOptions, context, rowType);
        this.keys = context.getKeys();
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.sqlCondition =
                LookupUtils.getSqlCondition(keys, options.tableName(), fieldNames, fieldTypes);
        LOG.info("Rdb sqlCondition = {}", this.sqlCondition);
    }

    public void setRdbSQLClient(SQLClient rdbSQLClient) {
        this.rdbSQLClient = rdbSQLClient;
    }

    @Override
    public void queryFromTable(
            CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        long start = System.nanoTime();
        JsonArray inputParams = new JsonArray();
        for (Object obj : inputs) {
            if (obj == null) {
                resultFuture.complete(null);
                return;
            }
            inputParams.add(obj.toString());
        }

        String key = String.valueOf(createCacheKey(inputs));
        rdbSQLClient.getConnection(
                conn -> {
                    if (conn.failed()) {
                        // Treatment failures
                        resultFuture.completeExceptionally(conn.cause());
                        lookupMetricsWrapper.rpcCallbackFailure();
                        return;
                    }

                    final SQLConnection connection = conn.result();
                    connection.queryWithParams(
                            this.sqlCondition,
                            inputParams,
                            rs -> {
                                if (rs.failed()) {
                                    lookupMetricsWrapper.rpcCallbackFailure();
                                    LOG.error(
                                            "Cannot retrieve the data from the database",
                                            rs.cause());
                                    resultFuture.complete(null);
                                    connection.close(
                                            done -> {
                                                if (done.failed()) {
                                                    throw new RuntimeException(done.cause());
                                                }
                                            });
                                    return;
                                }
                                List<RowData> cacheContent = Lists.newArrayList();
                                int resultSize = rs.result().getResults().size();
                                if (resultSize > 0) {
                                    List<RowData> rowList = Lists.newArrayList();
                                    for (JsonArray line : rs.result().getResults()) {
                                        RowData rowData =
                                                converter.deserializeString(
                                                        line.getList().toArray());
                                        if (enableCache()) {
                                            cacheContent.add(rowData);
                                        }
                                        rowList.add(rowData);
                                        lookupMetricsWrapper.sideJoinSuccess();
                                    }
                                    if (enableCache()) {
                                        putCache(
                                                key,
                                                CacheObj.buildCacheObj(
                                                        CacheContentType.MultiLine, cacheContent));
                                    }
                                    lookupMetricsWrapper.rtQuerySide(start);
                                    resultFuture.complete(rowList);
                                } else {
                                    resultFuture.complete(null);
                                    if (enableCache()) {
                                        putCache(key, CacheMissVal.getMissKeyObj());
                                    }
                                }
                                // and close the connection
                                connection.close(
                                        done -> {
                                            if (done.failed()) {
                                                throw new RuntimeException(done.cause());
                                            }
                                        });
                            });
                });
    }

    @Override
    protected Object createCacheKey(Object... inputs) {
        StringBuilder sb = new StringBuilder();
        for (Object obj : inputs) {
            if (obj != null) {
                sb.append(obj).append("_");
            }
        }
        return sb.toString();
    }
}
