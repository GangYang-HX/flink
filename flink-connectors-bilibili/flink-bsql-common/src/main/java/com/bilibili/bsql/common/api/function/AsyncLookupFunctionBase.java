package com.bilibili.bsql.common.api.function;

import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.common.cache.LRULookupCache;
import com.bilibili.bsql.common.cache.LookupCacheBase;
import com.bilibili.bsql.common.cache.cacheobj.CacheContentType;
import com.bilibili.bsql.common.cache.cacheobj.CacheObj;
import com.bilibili.bsql.common.converter.CustomRowConverter;
import com.bilibili.bsql.common.enums.CacheType;
import com.bilibili.bsql.common.metrics.LookupMetricsWrapper;
import com.bilibili.bsql.common.table.LookupOptionsEntityBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** AsyncSideFunctionBase. */
public abstract class AsyncLookupFunctionBase extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncLookupFunctionBase.class);

    protected int[][] keys;
    protected RowType dataType;
    protected LookupMetricsWrapper lookupMetricsWrapper;
    protected CustomRowConverter converter;
    protected Optional<LookupCacheBase> lookupCache;
    protected LookupOptionsEntityBase options;

    public AsyncLookupFunctionBase(
            LookupOptionsEntityBase options,
            LookupTableSource.LookupContext context,
            RowType rowType) {
        this.keys = context.getKeys();
        this.dataType = rowType;
        this.converter = new CustomRowConverter(rowType);
        this.options = options;
    }

    private Optional<LookupCacheBase> initCache(LookupOptionsEntityBase options) {
        if (options.cacheType() == null
                || CacheType.NONE.name().equalsIgnoreCase(options.cacheType())) {
            return Optional.empty();
        } else if (CacheType.LRU.name().equalsIgnoreCase(options.cacheType())) {
            LRULookupCache lruLookupCache =
                    new LRULookupCache(
                            options.cacheSize(), options.cacheTimeout(), options.cacheType());
            return Optional.of(lruLookupCache);
        } else {
            throw new RuntimeException(
                    "not support lookup cache with type: " + options.cacheType());
        }
    }

    protected boolean enableCache() {
        return this.lookupCache.isPresent();
    }

    protected Optional<CacheObj> getFromCache(Object key) {
        return this.lookupCache.map(lookupCacheBase -> lookupCacheBase.getFromCache(key));
    }

    protected void putCache(Object key, CacheObj value) {
        this.lookupCache.ifPresent(lookupCacheBase -> lookupCacheBase.putCache(key, value));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.lookupCache = initCache(options);
        this.lookupMetricsWrapper = new LookupMetricsWrapper(this.options.tableType(), context);
        this.converter.setRowConverterMetricGroup(context.getMetricGroup());
        LOG.info("Lookup table {} opens success.", this.options.tableName());
    }

    public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        lookupMetricsWrapper.tps();
        if (enableCache() && queryFromCache(resultFuture, inputs)) {
            return;
        }
        queryFromTable(resultFuture, inputs);
    }

    public void queryFromTable(
            CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        // do nothing
    }

    public boolean queryFromCache(
            CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        long start = System.nanoTime();
        Optional<CacheObj> val = getFromCache(createCacheKey(inputs));
        if (val.isPresent()) {
            if (CacheContentType.MissVal == val.get().getType()) {
                // miss value does not mean not found in cache,
                // but means in cache but not in external system.
                resultFuture.complete(null);
                lookupMetricsWrapper.sideJoinSuccess();
            } else if (CacheContentType.SingleLine == val.get().getType()) {
                RowData content = (RowData) val.get().getContent();
                resultFuture.complete(Collections.singleton(content));
                lookupMetricsWrapper.sideJoinSuccess();
                lookupMetricsWrapper.rtQuerySide(start);
            } else if (CacheContentType.MultiLine == val.get().getType()) {
                List<RowData> rowList = new ArrayList<>();
                rowList.addAll((List<RowData>) val.get().getContent());
                resultFuture.complete(rowList);
                lookupMetricsWrapper.sideJoinSuccess();
                lookupMetricsWrapper.rtQuerySide(start);
            } else {
                throw new RuntimeException("not support cache obj type " + val.get().getType());
            }
            return true;
        }
        return false;
    }

    protected abstract Object createCacheKey(Object... inputs);

    @Override
    public void close() throws Exception {
        super.close();
    }
}
