package com.bilibili.bsql.hdfs.function;

import com.bilibili.bsql.common.function.AsyncSideFunction;
import com.bilibili.bsql.hdfs.cache.HdfsCacheFactory;
import com.bilibili.bsql.hdfs.cache.HdfsTableCache;
import com.bilibili.bsql.hdfs.tableinfo.HdfsSideTableInfo;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HdfsLookupFunction.java
 * @description This is the description of HdfsLookupFunction.java
 * @createTime 2020-10-22 21:48:00
 */
public class HdfsLookupFunction extends AsyncSideFunction<HdfsSideTableInfo> {
    private final static Logger LOG = LoggerFactory.getLogger(HdfsLookupFunction.class);
    private HdfsTableCache tableCache;
    private final HdfsSideTableInfo sideTableInfo;
    public final int[][] keys;

    public HdfsLookupFunction(HdfsSideTableInfo sideTableInfo, LookupTableSource.LookupContext context) {
        super(sideTableInfo, context);
        this.sideTableInfo = (HdfsSideTableInfo) sideTableInfo;
        this.keys = context.getKeys();
    }

    @Override
    protected void open0(FunctionContext context) throws Exception {
        tableCache = HdfsCacheFactory.getInstance().getTableCache(sideTableInfo);
    }

    @Override
    protected void close0() throws Exception {
    }

    /**
     * 每一条流数据都会调用此方法进行join
     * inputs 为join key（on的字段）
     * 需要配合int[][] keys下标取值
     *
     * @param resultFuture
     * @param inputs
     * @throws Exception
     */
    @Override
    protected void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        sideMetricsWrapper.tps();
        long start = System.nanoTime();
        // 对应saber中，只取第一个sideInfo.getEqualValIndex().get(0)
        Object equalVal = inputs[0];
        if (equalVal == null) {
            resultFuture.complete(null);
            return;
        }
        Object[] ret = null;
        try {
            ret = tableCache.query(String.valueOf(equalVal));
            if (LOG.isDebugEnabled()) {
                LOG.debug("side query = " + Arrays.asList(null == ret ? new String[]{} : ret) + ", key = " + equalVal + ", key str = " + String.valueOf(equalVal));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (ret == null) {
            resultFuture.complete(null);
        } else {
            RowData rowData = converter.deserializeString(ret);
            resultFuture.complete(Collections.singletonList(rowData));
        }
        sideMetricsWrapper.sideJoinSuccess();
        sideMetricsWrapper.rtQuerySide(start);
    }

    /**
     * 没用父类的cache，不实现
     *
     * @param inputs
     * @return
     */
    @Override
    protected Object createCacheKey(Object... inputs) {
        return null;
    }
}
