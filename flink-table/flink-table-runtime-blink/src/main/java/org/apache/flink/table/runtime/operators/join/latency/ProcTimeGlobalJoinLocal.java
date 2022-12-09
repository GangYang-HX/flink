/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.latency;

import java.time.Clock;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
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

import org.apache.flink.table.runtime.operators.join.latency.config.GlobalJoinConfig;
import org.apache.flink.table.runtime.operators.join.latency.cache.CacheConfig;
import org.apache.flink.table.runtime.operators.join.latency.rockscache.CacheEntry;
import org.apache.flink.table.runtime.operators.join.latency.rockscache.CacheEntrySerializer;
import org.apache.flink.table.runtime.operators.join.latency.rockscache.RocksCache;

import org.apache.flink.table.runtime.operators.join.latency.util.SymbolsConstant;
import org.apache.flink.table.runtime.operators.join.latency.util.UnitConstant;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.StateTtlConfigUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.runtime.operators.join.latency.rockscache.CacheEntryType.*;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

/**
 * @author zhouxiaogang
 * @version $Id: ProcTimeLatencyJoinLocal.java, v 0.1 2021-03-11 20:11
 * zhouxiaogang Exp $$
 */
public class ProcTimeGlobalJoinLocal extends KeyedCoProcessFunction<RowData, RowData, RowData, RowData> {

	public static final String LABEL = "rocksDBJoin";

	private static final Logger LOGGER = LoggerFactory.getLogger(ProcTimeGlobalJoin.class);
	public static final String READABLE_SEP = "$";

	// from saber
	protected final long latencyInterval;
	protected final long windowInterval;
	protected final boolean isGlobalJoin;
	protected long redisBucketSize;
	protected int redisType;
	protected int compressType;
	protected String redisAddr;
	protected String mergeSideFunc;
	protected long stateTtl;

	// Minimum interval by which state is cleaned up
	protected final long allowedLateness;
	private final RowDataTypeInfo leftType;
	private final RowDataTypeInfo rightType;
	private final RowDataTypeInfo returnType;

	private ProcTimeGlobalJoinLocal.Metric metric;
	private ProcTimeGlobalJoinLocal.Metric timerMetric;

	private String operatorId;
	private RowDataKeySelector leftKeySelector;
	private RowDataKeySelector rightKeySelector;

	private transient OuterJoinPaddingUtil paddingUtil;
	private transient RowDataSerializer JoinedRowDeserializer;

	private transient MapState<String, CacheEntry> recordCache;
	private RocksCache directCache;
	private RocksCache rightCache;

	private boolean isMergeJoin;

//	private transient EmitAwareCollector joinCollector;

	public ProcTimeGlobalJoinLocal(
		FlinkJoinType joinType,
		GlobalJoinConfig config,
		RowDataTypeInfo leftType,
		RowDataTypeInfo rightType,
		RowDataTypeInfo returnType,
		GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> genJoinFunc,
		RowDataKeySelector leftKeySelector,
		RowDataKeySelector rightKeySelector) {

		this.latencyInterval = config.getDelayMs();
		this.windowInterval = config.getWindowLengthMs();
		this.isGlobalJoin = config.isGlobalLatency();
		this.redisAddr = config.getRedisAddress();
		this.redisBucketSize = config.getBucketNum();
		this.redisType = config.getRedisType();
		this.compressType = config.getCompressType();
		this.mergeSideFunc = config.getMergeSideFunc();
		this.stateTtl = config.getStateTtl();

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
		windowMetricGroup.gauge(
			"windowLength",
			new Gauge<Long>() {
				public Long getValue() {
					return windowInterval;
				}
			}
		);
		metric = new ProcTimeGlobalJoinLocal.Metric(windowMetricGroup);
		timerMetric = new ProcTimeGlobalJoinLocal.Metric(timerMetricGroup);
		CacheConfig cacheConfig = new CacheConfig();
		cacheConfig.setType(redisType);
		cacheConfig.setAddress(redisAddr);
		cacheConfig.setCompressType(compressType);
		cacheConfig.setBucketNum(redisBucketSize);

		LOGGER.info("redisType type : {} , compressType : {}, side function {}", redisType, compressType, mergeSideFunc);

		operatorId = ((StreamingRuntimeContext) getRuntimeContext()).getOperatorUniqueID();

		ExecutionConfig conf = getRuntimeContext().getExecutionConfig();

		MapStateDescriptor<String, CacheEntry> recordStoreDescriptor = new MapStateDescriptor<>(
			"LatencyRecordsKeys",
			StringSerializer.INSTANCE,
			new CacheEntrySerializer(
				new RowDataSerializer(conf, leftType.toRowType()),
				new RowDataSerializer(conf, rightType.toRowType()),
				new RowDataSerializer(conf, returnType.toRowType()))
		);

		if (stateTtl == 0) {
			stateTtl = Math.max(windowInterval, latencyInterval);
		}

		recordStoreDescriptor.enableTimeToLive(StateTtlConfigUtil.createTtlConfig(stateTtl));
		recordCache = getRuntimeContext().getMapState(recordStoreDescriptor);
		directCache = new RocksCache(recordCache, windowMetricGroup);

		MapStateDescriptor<String, CacheEntry> rightRecordStoreDescriptor = new MapStateDescriptor<>(
			"RightLatencyRecordsKeys",
			StringSerializer.INSTANCE,
			new CacheEntrySerializer(
				new RowDataSerializer(conf, leftType.toRowType()),
				new RowDataSerializer(conf, rightType.toRowType()),
				new RowDataSerializer(conf, returnType.toRowType()))
		);
		rightRecordStoreDescriptor.enableTimeToLive(StateTtlConfigUtil.createTtlConfig(stateTtl));
		MapState<String, CacheEntry> rightRecordCache = getRuntimeContext().getMapState(rightRecordStoreDescriptor);
		rightCache = new RocksCache(rightRecordCache, windowMetricGroup);

		paddingUtil = new OuterJoinPaddingUtil(leftType.getArity(), rightType.getArity());
		JoinedRowDeserializer = new RowDataSerializer(conf, returnType.toRowType());
	}

	@Override
	public void processElement1(RowData leftRow, Context ctx, Collector<RowData> out) throws Exception {

		RowData keyData = ctx.getCurrentKey();
		String leftKey = generateLeftKeyWithKeyFields(keyData);
		String rightKey = generateRightKeyWithKeyFields(keyData);

		if (directCache.exist(leftKey)) {
			return;
		}

		metric.in.inc();
		CacheEntry rightTableRow = rightCache.get(rightKey);
		if (rightTableRow != null) {
			rightCache.delete(rightKey);


			org.apache.flink.table.data.JoinedRowData joinedRowData = new org.apache.flink.table.data.JoinedRowData();
			joinedRowData.replace(leftRow, rightTableRow.getRow());
			metric.inJoin.inc();

			if (!isGlobalJoin) {
				out.collect(joinedRowData);
				return;
			} else {
				directCache.put(leftKey, new CacheEntry(joinedRowData, OUTPUT, System.currentTimeMillis()));
				ctx.timerService().registerProcessingTimeTimer((Clock.systemDefaultZone().millis() + windowInterval) / 3 * 3);
				metric.timerIn.inc();
				return;
			}
		}
		directCache.put(leftKey, new CacheEntry(leftRow, LEFT_INPUT, System.currentTimeMillis()));
		ctx.timerService().registerProcessingTimeTimer((Clock.systemDefaultZone().millis() + windowInterval) / 3 * 3);


		return;
	}

	@Override
	public void processElement2(RowData rightRow, Context ctx, Collector<RowData> out) throws Exception {
		metric.inRight.inc();
		RowData keyData = ctx.getCurrentKey();
		String leftKey = generateLeftKeyWithKeyFields(keyData);
		String rightKey = generateRightKeyWithKeyFields(keyData);

		CacheEntry leftTableRow = directCache.get(leftKey);
		if (leftTableRow != null) {

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
				CacheEntry resultCacheEntry = new CacheEntry(joinedRow, OUTPUT, leftTableRow.getTimestamp());
				directCache.put(leftKey, resultCacheEntry);
				return;
			}
		}

		CacheEntry row = new CacheEntry(rightRow, RIGHT_INPUT, System.currentTimeMillis());
		rightCache.put(rightKey, row);
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

	private RowData combineRightStreamKey(CacheEntry leftRow, RowData rightRow) throws Exception {
		if (leftRow.entryType == OUTPUT && !isMergeJoin) {
			return leftRow.getRow();
		}

		if (leftRow.entryType == LEFT_INPUT) {
			org.apache.flink.table.data.JoinedRowData newJoinedRow = new org.apache.flink.table.data.JoinedRowData();
			newJoinedRow.replace(leftRow.getRow(), rightRow);

			metric.inJoin.inc();

			return newJoinedRow;
		} else {
			assert leftRow.entryType == OUTPUT;
			RowData oldJoinedRow = leftRow.getRow();

			int rightArity = rightRow.getArity();
			int joinedArity = oldJoinedRow.getArity();
			int leftArity = joinedArity - rightArity;
			GenericRowData newJoinedRow = (GenericRowData) JoinedRowDeserializer.copyRowData(
				oldJoinedRow, new GenericRowData(oldJoinedRow.getArity()));


			for (int i = leftArity; i < joinedArity; i++) {
				StringBuffer stringBuffer = new StringBuffer();

				/**
				 * for the field not in the varchar type, just skip
				 * */
				if (rightType.getLogicalTypes()[i - leftArity].getTypeRoot() != VARCHAR) {
					continue;
				}
				String originRightValue = oldJoinedRow.getString(i).toString();
				stringBuffer.append(originRightValue == null ? "" : originRightValue);
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

		String leftKey = generateLeftKeyWithKeyFields(CachedData);

		CacheEntry row = directCache.get(leftKey);
		if (row == null || row.getRow() == null) {
			timerMetric.expire.inc();
			return;
		}

		timerMetric.del.inc();

		metric.out.inc();
		if (row.entryType == OUTPUT) {
			timerMetric.outJoin.inc();
			out.collect(row.getRow());
		} else if (row.entryType == LEFT_INPUT) {
			out.collect(paddingUtil.padLeft(row.getRow()));
		} else {
			throw new RuntimeException("unexpected cached row poped from cache!");
		}
	}

	@Override
	public void close() {
		// 分开关
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
