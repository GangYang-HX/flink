/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.latency;

import static org.apache.flink.table.runtime.operators.join.latency.cache.JoinRow.*;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

import java.io.IOException;
import java.time.Clock;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.operators.join.latency.cache.CacheConfig;
import org.apache.flink.table.runtime.operators.join.latency.cache.CacheFactory;
import org.apache.flink.table.runtime.operators.join.latency.cache.JoinCache;
import org.apache.flink.table.runtime.operators.join.latency.cache.JoinCacheEnums;
import org.apache.flink.table.runtime.operators.join.latency.cache.JoinRow;
import org.apache.flink.table.runtime.operators.join.latency.config.GlobalJoinConfig;
import org.apache.flink.table.runtime.operators.join.latency.serialize.JoinRowSerializer;
import org.apache.flink.table.runtime.operators.join.latency.util.SymbolsConstant;
import org.apache.flink.table.runtime.operators.join.latency.util.UnitConstant;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhouxiaogang
 * @version $Id: ProcTimeLatencyJoin.java, v 0.1 2020-11-20 16:01
 * zhouxiaogang Exp $$
 */
public class ProcTimeGlobalJoinWithTTL extends KeyedCoProcessFunction<RowData, RowData, RowData, RowData> {

	public static final String LABEL = "redisJoinWithTTL";

	private static final Logger LOGGER = LoggerFactory.getLogger(ProcTimeGlobalJoinWithTTL.class);
	public static final String READABLE_SEP = "$";

	private final FlinkJoinType joinType;

	// from saber
	protected final long latencyInterval;
	protected final long windowInterval;
	protected final boolean isGlobalJoin;
	protected long redisBucketSize;
	protected int redisType;
	protected int compressType;
	protected String redisAddr;
	protected String mergeSideFunc;
	private boolean saveTimeKey;

	// Minimum interval by which state is cleaned up
	protected final long allowedLateness;
	private final RowDataTypeInfo leftType;
	private final RowDataTypeInfo rightType;
	private final RowDataTypeInfo returnType;

	private JoinCache<JoinRow> directCache;
	private JoinCache<JoinRow> rightCache;
	private JoinCache<JoinRow> timerCache;

	private Metric metric;
	private Metric timerMetric;

	private String operatorId;
	private RowDataKeySelector leftKeySelector;
	private RowDataKeySelector rightKeySelector;

	private transient OuterJoinPaddingUtil paddingUtil;
	private transient RowDataSerializer JoinedRowDeserializer;

	private transient MapState<String, String> leftCache;

	private boolean isMergeJoin;

//	private transient EmitAwareCollector joinCollector;

	public ProcTimeGlobalJoinWithTTL(
		FlinkJoinType joinType,
		GlobalJoinConfig config,
		RowDataTypeInfo leftType,
		RowDataTypeInfo rightType,
		RowDataTypeInfo returnType,
		GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> genJoinFunc,
		RowDataKeySelector leftKeySelector,
		RowDataKeySelector rightKeySelector) {

		this.joinType = joinType;

		this.latencyInterval = config.getDelayMs();
		this.windowInterval = config.getWindowLengthMs();
		this.isGlobalJoin = config.isGlobalLatency();
		this.redisAddr = config.getRedisAddress();
		this.redisBucketSize = config.getBucketNum();
		this.redisType = config.getRedisType();
		this.compressType = config.getCompressType();
		this.mergeSideFunc = config.getMergeSideFunc();

		this.allowedLateness = 0;
		this.leftType = leftType;
		this.rightType = rightType;
		this.returnType = returnType;

		this.leftKeySelector = leftKeySelector;
		this.rightKeySelector = rightKeySelector;

		this.isMergeJoin = false;

		/**
		 * if is merge join, make sure all fields in right is varchar
		 * */
		if ("merge".equals(mergeSideFunc)) {
			isMergeJoin = true;
			for (LogicalType rowType : rightType.getLogicalTypes()) {
				if (rowType.getTypeRoot() != VARCHAR) {
					LOGGER.error("with merge join, right stream must only emit varchar type, but {} appear",
						rowType.getTypeRoot());
				}
			}
		}
	}


	@Override
	public void open(Configuration parameters) throws Exception {

		LOGGER.info("Instantiating LatencyJoinFunction: \n" +
				"window: {}, latency: {}, hold: {}",
			windowInterval,
			latencyInterval,
			isGlobalJoin
		);

		MetricGroup windowMetricGroup = getRuntimeContext().getMetricGroup().addGroup("latencyJoinWindow");
		MetricGroup timerMetricGroup = getRuntimeContext().getMetricGroup().addGroup("latencyJoinTimer");
		metric = new Metric(windowMetricGroup);
		timerMetric = new Metric(timerMetricGroup);
		CacheConfig cacheConfig = new CacheConfig();
		cacheConfig.setType(redisType);
		cacheConfig.setAddress(redisAddr);
		cacheConfig.setCompressType(compressType);
		cacheConfig.setBucketNum(redisBucketSize);

		MapStateDescriptor<String, String> leftMapStateDescriptor = new MapStateDescriptor<>(
			"LatencyJoinTimerKeys",
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);
		leftCache = getRuntimeContext().getMapState(leftMapStateDescriptor);

		LOGGER.info("redisType type : {} , compressType : {}, side function {}", redisType, compressType, mergeSideFunc);

		operatorId = ((StreamingRuntimeContext) getRuntimeContext()).getOperatorUniqueID();

		ExecutionConfig conf = getRuntimeContext().getExecutionConfig();


		directCache = CacheFactory.produce(
			cacheConfig,
			windowMetricGroup,
			new JoinRowSerializer(windowMetricGroup, leftType, rightType, returnType, conf)
		);

		CacheConfig ttlCacheConfig = new CacheConfig();
		ttlCacheConfig.setType(JoinCacheEnums.MULTI_SIMPLE.getCode());
		ttlCacheConfig.setAddress(redisAddr);
		ttlCacheConfig.setCompressType(compressType);
		ttlCacheConfig.setBucketNum(redisBucketSize);

		rightCache = CacheFactory.produce(
			ttlCacheConfig,
			windowMetricGroup,
			new JoinRowSerializer(windowMetricGroup, leftType, rightType, returnType, conf)
		);

		timerCache = CacheFactory.produce(
			cacheConfig,
			timerMetricGroup,
			new JoinRowSerializer(timerMetricGroup, leftType, rightType, returnType, conf)
		);
		paddingUtil = new OuterJoinPaddingUtil(leftType.getArity(), rightType.getArity());
		JoinedRowDeserializer = new RowDataSerializer(conf, returnType.toRowType());
	}

	@Override
	public void processElement1(RowData leftRow, Context ctx, Collector<RowData> out) throws Exception {

		RowData keyData = ctx.getCurrentKey();
		String leftKey = generateLeftKeyWithKeyFields(keyData);
		String rightKey = generateRightKeyWithKeyFields(keyData);
		String originalLeftKey = generateKeyString(leftKeySelector, keyData);

		if (saveTimeKey && isGlobalJoin) {
			String valueInState = null;
			try {
				valueInState = leftCache.get(originalLeftKey);
			}catch (Exception e) {
				LOGGER.info("rocks db get key failure", e);
			}

			if (valueInState != null) {
				return;
			}
		} else {
			if (directCache.exist(leftKey)) {
				return;
			}
		}

		metric.in.inc();
		JoinRow rightTableRow = rightCache.get(rightKey);
		if (rightTableRow != null) {
			rightCache.delete(rightKey);
//			joinFunction.join(leftRow, rightTableRow.getRow(), internalOutput);
//			JoinRow CombinedRow = mergeTableRowLeftIn(leftRow, rightTableRow, leftKey);

			org.apache.flink.table.data.JoinedRowData joinedRowData = new org.apache.flink.table.data.JoinedRowData();
			joinedRowData.replace(leftRow, rightTableRow.getRow());
			metric.inJoin.inc();

			if (!isGlobalJoin) {
				out.collect(joinedRowData);
				return;
			} else {
				directCache.put(leftKey, new JoinRow(joinedRowData, leftKey, OUTPUT), windowInterval);
				ctx.timerService().registerProcessingTimeTimer((Clock.systemDefaultZone().millis() + windowInterval) / 3 * 3);
				metric.timerIn.inc();
				return;
			}
		}
		directCache.put(leftKey, new JoinRow(leftRow, leftKey, LEFT_INPUT), windowInterval);
		ctx.timerService().registerProcessingTimeTimer((Clock.systemDefaultZone().millis() + windowInterval) / 3 * 3);
		if (saveTimeKey) {
			try {
				leftCache.put(originalLeftKey, "");
			}catch (Exception e) {
				LOGGER.info("rocks db put key failure", e);
			}
		}

		return;
	}

	@Override
	public void processElement2(RowData rightRow, Context ctx, Collector<RowData> out) throws Exception {
		metric.inRight.inc();
		RowData keyData = ctx.getCurrentKey();
		String leftKey = generateLeftKeyWithKeyFields(keyData);
		String rightKey = generateRightKeyWithKeyFields(keyData);

		JoinRow leftTableRow = directCache.get(leftKey);
		if (leftTableRow != null) {
//			org.apache.flink.table.data.JoinedRowData joinedRow = new org.apache.flink.table.data.JoinedRowData();
//			joinedRow.replace(leftTableRow.getRow(),rightRow);

			RowData joinedRow = combineRightStreamKey(leftTableRow, rightRow);

			if (!isGlobalJoin) {
				directCache.delete(leftKey);
				out.collect(joinedRow);
				return;
			} else {
				long passed = Clock.systemDefaultZone().millis() - leftTableRow.getTimestamp();
				long remainingExpireTime = windowInterval - passed;
				if (remainingExpireTime < UnitConstant.SECONDS2MILLS) {
					remainingExpireTime = UnitConstant.SECONDS2MILLS;
				}
				JoinRow resultJoinRow = new JoinRow(joinedRow, leftKey, OUTPUT);
				resultJoinRow.setTimestamp(leftTableRow.getTimestamp());
				directCache.put(leftKey, resultJoinRow, remainingExpireTime);
				return;
			}
		}

		JoinRow row = new JoinRow(rightRow, rightKey, RIGHT_INPUT);
		JoinRow existingRow;
		long s = System.nanoTime();
		//if right flow exists the key,then combine
		if (isMergeJoin && (existingRow=rightCache.get(rightKey)) !=null ){
			RowData mergedRow = combineRightDuplicateKey(existingRow.getRow(), rightRow);
			row.setRow(mergedRow);

			long passed = Clock.systemDefaultZone().millis() - existingRow.getTimestamp();
			long remainingExpireTime = windowInterval - passed;
			if (remainingExpireTime < UnitConstant.SECONDS2MILLS) {
				remainingExpireTime = UnitConstant.SECONDS2MILLS;
			}
			row.setTimestamp(existingRow.getTimestamp());
			rightCache.put(rightKey, row, remainingExpireTime);
			return;
		}
		rightCache.put(rightKey, row, latencyInterval);
	}

	private RowData combineRightDuplicateKey(RowData rightRow1, RowData rightRow2){
		GenericRowData newJoinedRow = new GenericRowData(rightRow1.getArity());
		int size = 0;
		for (int i = 0; i < rightRow1.getArity(); i++){
			StringBuilder stringBuilder = new StringBuilder();
			if (rightType.getLogicalTypes()[i].getTypeRoot() != VARCHAR) {
				continue;
			}
			String originRightValue = rightRow1.getString(i).toString();
			stringBuilder.append(originRightValue==null ? "" : originRightValue);
			stringBuilder.append(SymbolsConstant.COMMA);
			stringBuilder.append(rightRow2.getString(i).toString());

			newJoinedRow.setField(i, StringData.fromString(stringBuilder.toString()));
			size += stringBuilder.length();
		}
		if (size > 1024 * 1024 * 2){
			LOGGER.warn("After right stream merging，the size : {} kb is greater than 2MB and the key is : {}",size/1024,rightRow1.getString(0));
		}
		return newJoinedRow;
	}

	private String generateKeyString(RowDataKeySelector KeySelector, RowData keyData) throws Exception {
		RowDataTypeInfo dataTypeInfo = KeySelector.getProducedType();

		/**
		 * validation of only one key allowed is in the rule
		 * */
		LogicalType logicalType = dataTypeInfo.getLogicalTypes()[0];
		switch (logicalType.getTypeRoot()) {
			case BIGINT:
				return String.valueOf(keyData.getLong(0));
			case VARCHAR:
				return keyData.getString(0).toString();
			case INTEGER:
				return String.valueOf(keyData.getInt(0));
			default:
				throw new UnsupportedOperationException("latency join only support for long, varchar, integer join");
		}

	}

	private String generateLeftKeyWithKeyFields(RowData keyData) throws Exception {
		StringBuffer sb = new StringBuffer();
		sb.append(generateKeyString(leftKeySelector, keyData));

		sb.append(READABLE_SEP);
		sb.append(operatorId);
		sb.append(READABLE_SEP);
		sb.append(0);

		return sb.toString();
	}

	private String generateRightKeyWithKeyFields(RowData keyData) throws Exception {
		StringBuffer sb = new StringBuffer();
		sb.append(generateKeyString(rightKeySelector, keyData));

		sb.append(READABLE_SEP);
		sb.append(operatorId);
		sb.append(READABLE_SEP);
		sb.append(1);

		return sb.toString();
	}

	private RowData combineRightStreamKey(JoinRow leftRow, RowData rightRow) throws Exception {
		if (OUTPUT.equals(leftRow.getTableName()) && !isMergeJoin) {
			return leftRow.getRow();
		}

		if (LEFT_INPUT.equals(leftRow.getTableName())) {
			org.apache.flink.table.data.JoinedRowData newJoinedRow = new org.apache.flink.table.data.JoinedRowData();
			newJoinedRow.replace(leftRow.getRow(), rightRow);

			metric.inJoin.inc();

			return newJoinedRow;
		} else {
			assert OUTPUT.equals(leftRow.getTableName());
			RowData oldJoinedRow = leftRow.getRow();

			int rightArity = rightRow.getArity();
			int joinedArity = oldJoinedRow.getArity();
			int leftArity = joinedArity - rightArity;
			GenericRowData newJoinedRow = (GenericRowData) JoinedRowDeserializer.copyRowData(
				oldJoinedRow, new GenericRowData(oldJoinedRow.getArity()));


			for (int i = leftArity; i < joinedArity; i++){
				StringBuffer stringBuffer = new StringBuffer();

				/**
				 * for the field not in the varchar type, just skip
				 * */
				if (rightType.getLogicalTypes()[i - leftArity].getTypeRoot() != VARCHAR) {
					continue;
				}
				String originRightValue = oldJoinedRow.getString(i).toString();
				stringBuffer.append(originRightValue==null ? "" : originRightValue);
				stringBuffer.append(SymbolsConstant.COMMA);
				stringBuffer.append(rightRow.getString(i - leftArity).toString());

				newJoinedRow.setField(i, StringData.fromString(stringBuffer.toString()));
			}
			return newJoinedRow;
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
		timerMetric.timerOut.inc();
		// 这个实际上是只有key的part
		RowData CachedData = ctx.getCurrentKey();

		String key = generateKeyString(leftKeySelector, CachedData);
		if (saveTimeKey) {
			try {
				leftCache.remove(key);
			}catch (Exception e) {
				LOGGER.info("rocks db remove key failure", e);
			}
		}

		String leftKey = generateLeftKeyWithKeyFields(CachedData);

		JoinRow row = timerCache.get(leftKey);
		if (row == null || row.getRow() == null) {
			timerMetric.expire.inc();
			return;
		}
		if (redisType == JoinCacheEnums.BUCKET.getCode() || redisType == JoinCacheEnums.MULTI_BUCKET.getCode()) {
			timerCache.delete(row.getCondition());
			timerMetric.del.inc();
		}
		metric.out.inc();
		if (OUTPUT.equals(row.getTableName())) {
			timerMetric.outJoin.inc();
			out.collect(row.getRow());
		} else if (LEFT_INPUT.equals(row.getTableName())) {
			out.collect(paddingUtil.padLeft(row.getRow()));
		} else {
			throw new RuntimeException("unexpected cached row poped from cache!");
		}
	}

	@Override
	public void close() {
		// 分开关
		try {
			if (timerCache!=null) {
				timerCache.close();
			}
		} catch (IOException e) {
			LOGGER.info("close cache error:", e);
		}
		try {
			if (directCache!=null) {
				directCache.close();
			}
		} catch (IOException e) {
			LOGGER.info("close cache error:", e);
		}
	}


	private static class Metric {
		private Counter in;
		private Counter inRight;
		private Counter out;
		private Counter del;
		private Counter inJoin;
		private Counter outJoin;
		private Counter expire;
		private Counter timerIn;
		private Counter timerOut;
		private Histogram timerInRT;


		private Metric(MetricGroup metricGroup) {
			in = metricGroup.counter("in");
			inRight = metricGroup.counter("inRight");
			inJoin = metricGroup.counter("inJoin");
			out = metricGroup.counter("out");
			del = metricGroup.counter("delete");
			outJoin = metricGroup.counter("outJoin");
			expire = metricGroup.counter("expire");
			timerIn = metricGroup.counter("timerIn");
			timerOut = metricGroup.counter("timerOut");
			timerInRT = metricGroup.histogram("timerInRT", new DescriptiveStatisticsHistogram(DefaultMetricsConfig.DEFAULT_WINDOW_SIZE));

		}
	}

}
