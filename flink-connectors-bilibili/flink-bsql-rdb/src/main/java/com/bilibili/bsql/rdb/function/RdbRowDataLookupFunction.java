package com.bilibili.bsql.rdb.function;

import com.bilibili.bsql.common.cache.cachobj.CacheContentType;
import com.bilibili.bsql.common.cache.cachobj.CacheMissVal;
import com.bilibili.bsql.common.cache.cachobj.CacheObj;
import com.bilibili.bsql.common.function.AsyncSideFunction;
import com.bilibili.bsql.common.metrics.SideMetricsWrapper;
import com.bilibili.bsql.rdb.tableinfo.RdbSideTableInfo;

import com.google.common.collect.Lists;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;

import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RdbRowDataLookupFunction.java
 * @description This is the description of RdbRowDataLookupFunction.java
 * @createTime 2020-10-20 14:21:00
 */
public class RdbRowDataLookupFunction extends AsyncSideFunction<RdbSideTableInfo> {
    private static final long serialVersionUID = 2098635244857937720L;
    private static final Logger LOG = LoggerFactory.getLogger(RdbRowDataLookupFunction.class);

    private transient SQLClient rdbSQLClient;
    public RdbSideTableInfo sideTableInfo;
    public int[][] keys;
    public final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;
    public final static int DEFAULT_VERTX_WORKER_POOL_SIZE = 10;
    public final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public RdbRowDataLookupFunction(RdbSideTableInfo sideTableInfo, LookupTableSource.LookupContext context) {
        super(sideTableInfo, context);
        this.sideTableInfo = sideTableInfo;
        this.keys = context.getKeys();
    }

    @Override
    public void open0(FunctionContext context) throws Exception {
    }

    @Override
    public void close0() throws Exception {
    }

    public void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
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
        rdbSQLClient.getConnection(conn -> {
            if (conn.failed()) {
                //Treatment failures
                resultFuture.completeExceptionally(conn.cause());
                sideMetricsWrapper.rpcCallbackFailure();
                return;
            }

            final SQLConnection connection = conn.result();
            String sqlCondition = sideTableInfo.getSqlCondition(keys);
            connection.queryWithParams(sqlCondition, inputParams, rs -> {
                if (rs.failed()) {
                    sideMetricsWrapper.rpcCallbackFailure();
                    LOG.error("Cannot retrieve the data from the database", rs.cause());
                    resultFuture.complete(null);
                    connection.close(done -> {
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
                        RowData rowData = converter.deserializeString(line.getList().toArray());
                        if (openCache()) {
                            cacheContent.add(rowData);
                        }
                        rowList.add(rowData);
                        sideMetricsWrapper.sideJoinSuccess();
                    }
                    if (openCache()) {
                        putCache(key, CacheObj.buildCacheObj(CacheContentType.MultiLine, cacheContent));
                    }
                    sideMetricsWrapper.rtQuerySide(start);
                    resultFuture.complete(rowList);
                } else {
                    resultFuture.complete(null);
                    if (openCache()) {
                        putCache(key, CacheMissVal.getMissKeyObj());
                    }
                }
                // and close the connection
                connection.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    }
                });
            });
        });
    }

    public void setRdbSQLClient(SQLClient rdbSQLClient) {
        this.rdbSQLClient = rdbSQLClient;
    }

    protected Object createCacheKey(Object... inputs) {
        StringBuilder sb = new StringBuilder();
        for (Object obj : inputs) {
            if (obj != null) {
                sb.append(obj.toString()).append("_");
            }
        }
        return sb.toString();
    }
}
