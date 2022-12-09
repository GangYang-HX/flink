package com.bilibili.bsql.hdfs.function;

import com.bilibili.bsql.common.function.AsyncSideFunction;
import com.bilibili.bsql.hdfs.cache.HiveCacheFactory;
import com.bilibili.bsql.hdfs.tableinfo.HiveSideTableInfo;
import com.bilibili.bsql.hdfs.cache.HiveTableCache;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @Author: JinZhengyu
 * @Date: 2022/7/28 下午3:02
 */
public class HiveLookupFunction extends AsyncSideFunction<HiveSideTableInfo> {
    private final static Logger log = LoggerFactory.getLogger(HiveLookupFunction.class);
    private final HiveSideTableInfo sideTableInfo;
    private HiveTableCache tableCache;

    public HiveLookupFunction(HiveSideTableInfo sideTableInfo, LookupTableSource.LookupContext context) {
        super(sideTableInfo, context);
        this.sideTableInfo = sideTableInfo;

    }

    @Override
    protected void open0(FunctionContext context) throws Exception {
        tableCache = HiveCacheFactory.getInstance().getTableCache(sideTableInfo);
    }

    @Override
    protected void close0() throws Exception {

    }

    @Override
    protected void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        long start = System.nanoTime();
        Object equalVal = inputs[0];
        if (equalVal == null) {
            resultFuture.complete(null);
            return;
        }
        Object[] ret;
        try {
            ret = tableCache.query(String.valueOf(equalVal), sideTableInfo);
            if (ret == null) {
                resultFuture.complete(null);
                sideMetricsWrapper.sideJoinMiss();
                if (sideTableInfo.isTurnOnJoinLog()){
                    log.warn("join dimension table:{} does not hit the joinKey data:{}.", sideTableInfo.getName(), equalVal);
                }
            } else {
                RowData rowData = converter.deserializeString(ret);
                resultFuture.complete(Collections.singleton(rowData));
                sideMetricsWrapper.sideJoinSuccess();
            }
        } catch (Exception e) {
            if (sideTableInfo.isTurnOnJoinLog()){
                log.error("An exception was encountered during the table:{} join process.", sideTableInfo.getName(), e);
            }
            resultFuture.complete(null);
            sideMetricsWrapper.sideJoinException();
        }

        sideMetricsWrapper.rtQuerySide(start);
    }

    @Override
    protected Object createCacheKey(Object... inputs) {
        return null;
    }

}
