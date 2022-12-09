package com.bilibili.bsql.common.function;

import com.bilibili.bsql.common.SideTableInfo;
import com.bilibili.bsql.common.cache.AbsSideCache;
import com.bilibili.bsql.common.cache.cachobj.CacheContentType;
import com.bilibili.bsql.common.cache.cachobj.CacheObj;
import com.bilibili.bsql.common.cache.LRUSideCache;
import com.bilibili.bsql.common.enums.CacheType;
import com.bilibili.bsql.common.format.converter.CustomRowConverter;
import com.bilibili.bsql.common.metrics.SideMetricsWrapper;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className AsyncSideFunction.java
 * @description This is the description of AsyncSideFunction.java
 * @createTime 2020-10-20 11:01:00
 */
public abstract class AsyncSideFunction<ST extends SideTableInfo> extends AsyncTableFunction<RowData> {

	private final static Logger LOG = LoggerFactory.getLogger(AsyncSideFunction.class);
	private static final long serialVersionUID = 2098635244857937717L;

	protected ST sideTableInfo;
	protected int[][] keys;
//	protected DynamicTableSource.DataStructureConverter converter;
	protected RowType dataType;
	protected CustomRowConverter converter;

	protected SideMetricsWrapper sideMetricsWrapper;

	public AsyncSideFunction(ST sideTableInfo, LookupTableSource.LookupContext context) {
		this.sideTableInfo = sideTableInfo;
		this.keys = context.getKeys();
//		this.converter
//			= context.createDataStructureConverter(sideTableInfo.getPhysicalRowDataType());
		this.dataType = (RowType) sideTableInfo.getPhysicalRowDataType().getLogicalType();
		this.converter = new CustomRowConverter(dataType);
	}

	private void initCache() {
		if (sideTableInfo.getCacheType() == null || CacheType.NONE.name().equalsIgnoreCase(sideTableInfo.getCacheType())) {
			return;
		}
		AbsSideCache sideCache;
		if (CacheType.LRU.name().equalsIgnoreCase(sideTableInfo.getCacheType())) {
			sideCache = new LRUSideCache(sideTableInfo);
			sideTableInfo.setSideCache(sideCache);
		} else {
			throw new RuntimeException("not support side cache with type: " + sideTableInfo.getCacheType());
		}
		sideCache.initCache();
	}

	protected CacheObj getFromCache(Object key) {
		return sideTableInfo.getSideCache().getFromCache(key);
	}

	protected void putCache(Object key, CacheObj value) {
		sideTableInfo.getSideCache().putCache(key, value);
	}

	protected boolean openCache() {
		return sideTableInfo.getSideCache() != null;
	}

	@Override
	public final void open(FunctionContext context) throws Exception {
		super.open(context);
		initCache();
		this.sideMetricsWrapper = new SideMetricsWrapper(sideTableInfo.getType(), context);
		this.converter.setRowConverterMetricGroup(context.getMetricGroup());
		open0(context);
		LOG.info("side table {} opens success", sideTableInfo.getName());
	}

	public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
		sideMetricsWrapper.tps();
		if (openCache()) {
			long start = System.nanoTime();
			CacheObj val = getFromCache(createCacheKey(inputs));
			if (val != null) {
				if (CacheContentType.MissVal == val.getType()) {
					/**
					 * miss value does not mean not found in cache, but means in cache but not in external system
					 * */
					resultFuture.complete(null);
					sideMetricsWrapper.sideJoinSuccess();
				} else if (CacheContentType.SingleLine == val.getType()) {
					RowData content = (RowData) val.getContent();
					resultFuture.complete(Collections.singleton(content));
					sideMetricsWrapper.sideJoinSuccess();
					sideMetricsWrapper.rtQuerySide(start);
				} else if (CacheContentType.MultiLine == val.getType()) {
					List<RowData> rowList = new ArrayList<>();
					for (RowData jsonArray : (List<RowData>) val.getContent()) {
						rowList.add(jsonArray);
					}
					resultFuture.complete(rowList);
					sideMetricsWrapper.sideJoinSuccess();
					sideMetricsWrapper.rtQuerySide(start);
				} else {
					throw new RuntimeException("not support cache obj type " + val.getType());
				}
				return;
			}
		}
		eval0(resultFuture, inputs);
	}

	@Override
	public final void close() throws Exception {
		super.close();
		close0();
	}

	protected abstract void open0(FunctionContext context) throws Exception;

	protected abstract void close0() throws Exception;

	protected abstract void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs);

	protected abstract Object createCacheKey(Object... inputs);
}
