
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.latency.v2;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.OuterJoinPaddingUtil;
import org.apache.flink.table.runtime.operators.join.latency.v2.config.LatencyJoinConfig;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.StateTtlConfigUtil;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A CoProcessFunction to execute time interval (time-bounded) stream inner-join.
 * Two kinds of time criteria:
 * "L.time between R.time + X and R.time + Y" or "R.time between L.time - Y and L.time - X"
 * X and Y might be negative or positive and X <= Y.
 */
abstract class TimeLatencyJoin extends KeyedCoProcessFunction<RowData, RowData, RowData, RowData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeLatencyJoin.class);

    protected final long leftRelativeSize;
    protected final long rightRelativeSize;
    protected final long allowedLateness;
    protected final long ttlTime;
	private final boolean keyedStateTtlEnable;
	private final long cleanUpInterval;
    private final RowDataTypeInfo leftType;
    private final RowDataTypeInfo rightType;

    public transient ValueState<Boolean> leftKeyCache;
    public transient ValueState<Boolean> rightKeyCache;

    protected long leftOperatorTime = 0L;
    protected long rightOperatorTime = 0L;

    private transient OuterJoinPaddingUtil paddingUtil;

    private transient MapState<Long, List<RowData>> leftCache;

    private transient MapState<Long, List<RowData>> rightCache;

    private final boolean joinMergeEnable;
    private final String joinMergeDelimiter;
    private final boolean isDistinctJoin;
    private final boolean isGlobalJoin;

    private final boolean isTtlEnable;

    TimeLatencyJoin(
            FlinkJoinType joinType,
            long leftLowerBound,
            long leftUpperBound,
            long allowedLateness,
            RowDataTypeInfo leftType,
            RowDataTypeInfo rightType,
            GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> genJoinFunc,
            LatencyJoinConfig joinConfig) {

        //只支持left join,非left join直接报错
        if (joinType != FlinkJoinType.LEFT) {
            throw new UnsupportedOperationException("this join only support left join type");
        }
        this.leftRelativeSize = -leftLowerBound;
        this.rightRelativeSize = leftUpperBound;
        if (allowedLateness < 0) {
            throw new IllegalArgumentException("The allowed lateness must be non-negative.");
        }
        this.allowedLateness = allowedLateness;

        isTtlEnable = joinConfig.isTtlEnable();
        keyedStateTtlEnable = joinConfig.isKeyedStateTtlEnable();
        if (leftLowerBound < 0) {
            leftLowerBound = -leftLowerBound;
        }
        if (leftUpperBound < 0) {
            leftUpperBound = -leftUpperBound;
        }

		// minCleanUpInterval = (leftRelativeSize + rightRelativeSize) / 2;
		if (joinConfig.getCleanUpInterval() != -1) {
			this.cleanUpInterval = joinConfig.getCleanUpInterval();
		} else {
			this.cleanUpInterval = (leftRelativeSize + rightRelativeSize) / 2;
		}
		this.ttlTime = Math.max(leftLowerBound, leftUpperBound) + cleanUpInterval + allowedLateness + 10;

        this.leftType = leftType;
        this.rightType = rightType;

        joinMergeEnable = joinConfig.isJoinMergeEnable();
        joinMergeDelimiter = joinConfig.getJoinMergeDelimiter();
        if (joinMergeEnable) {
            for (TypeInformation<?> colType : rightType.getFieldTypes()) {
                boolean isVarchar = colType.getTypeClass() == String.class;
                boolean isTimeIndicator = colType instanceof TimeIndicatorTypeInfo;
                if (!isVarchar && !isTimeIndicator) {
                    throw new UnsupportedOperationException("when merge enable, all row fields must be varchar");
                }
            }
        }
        isDistinctJoin = joinConfig.isDistinctJoin();
        isGlobalJoin = joinConfig.isGlobalJoin();

        LOGGER.info("LatencyJoinV2 config: " +
                "joinMergeEnable: {}, " +
                "joinMergeDelimiter: {}, " +
                "isDistinctJoin: {}, " +
                "isGlobalJoin: {}, " +
                "ttlEnable: {}.", joinMergeEnable, joinMergeDelimiter, isDistinctJoin, isGlobalJoin, isTtlEnable);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> leftKeyCacheStateDescriptor = new ValueStateDescriptor<>(
                "LatencyJoinLeftKeyCache",
                BasicTypeInfo.BOOLEAN_TYPE_INFO,
                false);
        ValueStateDescriptor<Boolean> rightKeyCacheStateDescriptor = new ValueStateDescriptor<>(
                "LatencyJoinRightKeyCache",
                BasicTypeInfo.BOOLEAN_TYPE_INFO,
                false);
		if (keyedStateTtlEnable && ttlTime != 0) {
			leftKeyCacheStateDescriptor.enableTimeToLive(StateTtlConfigUtil.createTtlConfig(ttlTime));
			rightKeyCacheStateDescriptor.enableTimeToLive(StateTtlConfigUtil.createTtlConfig(ttlTime));
		}
        leftKeyCache = getRuntimeContext().getState(leftKeyCacheStateDescriptor);
        rightKeyCache = getRuntimeContext().getState(rightKeyCacheStateDescriptor);

        // Initialize the data caches.
        ListTypeInfo<RowData> leftRowTypeInfo = new ListTypeInfo<>(leftType);
        MapStateDescriptor<Long, List<RowData>> leftMapStateDescriptor = new MapStateDescriptor<>(
                "LatencyJoinLeftCache",
                BasicTypeInfo.LONG_TYPE_INFO,
                leftRowTypeInfo);
		if (keyedStateTtlEnable && ttlTime != 0) {
			leftMapStateDescriptor.enableTimeToLive(StateTtlConfigUtil.createTtlConfig(ttlTime));
		}
        leftCache = getRuntimeContext().getMapState(leftMapStateDescriptor);

        ListTypeInfo<RowData> rightRowTypeInfo = new ListTypeInfo<>(rightType);
        MapStateDescriptor<Long, List<RowData>> rightMapStateDescriptor = new MapStateDescriptor<>(
                "LatencyJoinRightCache",
                BasicTypeInfo.LONG_TYPE_INFO,
                rightRowTypeInfo);
		if ((keyedStateTtlEnable || isTtlEnable) && ttlTime != 0) {
            rightMapStateDescriptor.enableTimeToLive(StateTtlConfigUtil.createTtlConfig(ttlTime));
        }
        rightCache = getRuntimeContext().getMapState(rightMapStateDescriptor);
        paddingUtil = new OuterJoinPaddingUtil(leftType.getArity(), rightType.getArity());

    }

    @Override
    public void processElement1(RowData leftRow, Context ctx, Collector<RowData> out) throws Exception {
        updateOperatorTime(ctx);
        long timeForLeftRow = getTimeForLeftStream(ctx, leftRow);
        if (isDistinctJoin) {
			Boolean exists = leftKeyCache.value();
			if (exists != null && exists) {
                return;
            }
            leftKeyCache.update(true);
        }

        if (!isGlobalJoin) {
            // 如果不是 global join，左流立即 join 到右流后输出，需要考虑把keyCache清理掉，右流也清理掉
            // 左流 join 到的右流可能是多条的，是个列表，直接看情况走 merge
            // 找到右流过期需要弹出的数据,右流最多比左流早rightRelativeSize
            // long leftExpireTime = calExpirationTime(leftOperatorTime, leftRelativeSize);
            long rightEarliestTime = calExpirationTime(leftOperatorTime, rightRelativeSize);
            Iterator<Map.Entry<Long, List<RowData>>> rightIterator = rightCache.iterator();
            Map<Long, List<RowData>> rightExpireMap = new HashMap<>();
            while (rightIterator.hasNext()) {
                Map.Entry<Long, List<RowData>> rightEntry = rightIterator.next();
                Long rightTime = rightEntry.getKey();
                if (rightTime >= rightEarliestTime) {
                    List<RowData> rightRowDataList = rightEntry.getValue();
                    rightExpireMap.put(rightTime, rightRowDataList);
                    rightIterator.remove();
                }
            }

            List<RowData> rightMatchRowData = new ArrayList<>(1);
            for (Map.Entry<Long, List<RowData>> rightEntry : rightExpireMap.entrySet()) {
                rightMatchRowData.addAll(rightEntry.getValue());
            }
            // 找到可以 join 的右流数据，根据情况看是不是需要 merge，join 完以后就直接返回了
            if (rightMatchRowData.size() > 0) {
                if (joinMergeEnable) {
                    RowData mergedRightRow = combineRightRowData(rightMatchRowData);
                    out.collect(new JoinedRowData(leftRow, mergedRightRow));
                }
                else {
                    out.collect(new JoinedRowData(leftRow, rightMatchRowData.get(0)));
                }

                // 如果不是distinctJoin，keyCache 用不到，所以不会有影响
				leftKeyCache.clear();
				rightKeyCache.clear();

                // join到以后不用再走 Timer 注册逻辑，直接返回
                return;
            }
            // 如果找不到可以 join 的数据，将左流数据加入到Cache中。
        }

        // 不管条件，是否延时到达，是否超时等等，直接加入到cache中，join后置到onTimer方法中操作
        List<RowData> leftRowList = leftCache.get(timeForLeftRow);
        if (leftRowList == null) {
            leftRowList = new ArrayList<>(1);
        }
        leftRowList.add(leftRow);
        leftCache.put(timeForLeftRow, leftRowList);

        long cleanUpTime = timeForLeftRow + leftRelativeSize + allowedLateness + 1;
        registerTimer(ctx, cleanUpTime);
    }

    @Override
    public void processElement2(RowData rightRow, Context ctx, Collector<RowData> out) throws Exception {
        updateOperatorTime(ctx);
        long timeForRightRow = getTimeForRightStream(ctx, rightRow);
        if (!joinMergeEnable && isDistinctJoin) {
			Boolean exists = rightKeyCache.value();
			if (exists != null && exists) {
                return;
            }
            rightKeyCache.update(true);
        }

        if (!isGlobalJoin) {
            // 如果不是 global join，左流立即 join 到右流后输出，需要考虑把keyCache清理掉，右流也清理掉
            // 右流join 到的左流一定是唯一的
            // 找到右流过期需要弹出的数据,右流最多比左流早leftRelativeSize
            // long rightExpireTime = calExpirationTime(rightOperatorTime, rightRelativeSize);
            long leftEarliestTime = calExpirationTime(rightOperatorTime, leftRelativeSize);
            Iterator<Map.Entry<Long, List<RowData>>> leftIterator = leftCache.iterator();
            Map<Long, List<RowData>> leftExpireMap = new HashMap<>();
            while (leftIterator.hasNext()) {
                Map.Entry<Long, List<RowData>> leftEntry = leftIterator.next();
                Long leftTime = leftEntry.getKey();
                if (leftTime >= leftEarliestTime) {
                    List<RowData> leftRowDataList = leftEntry.getValue();
                    leftExpireMap.put(leftTime, leftRowDataList);
                    leftIterator.remove();
                }
            }

            List<RowData> leftMatchRowData = new ArrayList<>(1);
            for (Map.Entry<Long, List<RowData>> leftEntry : leftExpireMap.entrySet()) {
                leftMatchRowData.addAll(leftEntry.getValue());
            }
            // 找到可以 join 的右流数据，根据情况看是不是需要 merge，join 完以后就直接返回了
            if (leftMatchRowData.size() > 0) {
                // 不用关心是否 merge，能 join 到的话，左流和右流一定是唯一的，首先左流是唯一的，其次右流不可能和左流同时存在 cache 中。
                out.collect(new JoinedRowData(leftMatchRowData.get(0), rightRow));

                // 如果不是distinctJoin，keyCache 用不到，所以不会有影响
                leftKeyCache.clear();
                rightKeyCache.clear();

                // join到以后不用再走 Timer 注册逻辑，直接返回
                return;
            }
            // 如果找不到可以 join 的数据，将右流数据加入到Cache中。
        }
        // 不管条件，是否延时到达，是否超时等等，直接加入到cache中，join后置到onTimer方法中操作
        List<RowData> rightRowList = rightCache.get(timeForRightRow);
        if (rightRowList == null) {
            rightRowList = new ArrayList<>(1);
        }
        rightRowList.add(rightRow);
        rightCache.put(timeForRightRow, rightRowList);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
        updateOperatorTime(ctx);

        //找到左流过期需要弹出的数据
        long leftExpireTime = calExpirationTime(leftOperatorTime, leftRelativeSize);
        Iterator<Map.Entry<Long, List<RowData>>> leftIterator = leftCache.iterator();
        Map<Long, List<RowData>> leftExpireMap = new HashMap<>();
        while (leftIterator.hasNext()) {
            Map.Entry<Long, List<RowData>> leftEntry = leftIterator.next();
            Long leftTime = leftEntry.getKey();
            if (leftTime <= leftExpireTime) {
                List<RowData> leftRowDataList = leftEntry.getValue();
                leftExpireMap.put(leftTime, leftRowDataList);
                leftIterator.remove();
            }
        }

        //找到右流过期需要弹出的数据,右流最多比左流早rightRelativeSize
        long rightEarliestTime = calExpirationTime(leftExpireTime, rightRelativeSize);
        Iterator<Map.Entry<Long, List<RowData>>> rightIterator = rightCache.iterator();
        Map<Long, List<RowData>> rightExpireMap = new HashMap<>();
        while (rightIterator.hasNext()) {
            Map.Entry<Long, List<RowData>> rightEntry = rightIterator.next();
            Long rightTime = rightEntry.getKey();
            if (rightTime >= rightEarliestTime) {
                List<RowData> rightRowDataList = rightEntry.getValue();
                rightExpireMap.put(rightTime, rightRowDataList);
                rightIterator.remove();
            }
        }

        for (Map.Entry<Long, List<RowData>> leftEntry : leftExpireMap.entrySet()) {
            List<RowData> rightMatchRowData = new ArrayList<>(1);
            for (Map.Entry<Long, List<RowData>> rightEntry : rightExpireMap.entrySet()) {
                rightMatchRowData.addAll(rightEntry.getValue());
            }

            if (rightMatchRowData.size() == 0) {
                for (RowData leftRow : leftEntry.getValue()) {
                    out.collect(paddingUtil.padLeft(leftRow));
                }
            } else if (joinMergeEnable) {
                RowData mergedRightRow = combineRightRowData(rightMatchRowData);
                for (RowData leftRow : leftEntry.getValue()) {
                    out.collect(new JoinedRowData(leftRow, mergedRightRow));
                }
            } else {
                for (RowData leftRow : leftEntry.getValue()) {
                    for (RowData rightRow : rightMatchRowData) {
                        out.collect(new JoinedRowData(leftRow, rightRow));
                    }
                }
            }
        }

        if (isDistinctJoin) {
			leftKeyCache.clear();
			rightKeyCache.clear();
        }
    }

    private RowData combineRightRowData(List<RowData> rowDataList) {
        GenericRowData mergedRowData = new GenericRowData(rightType.getArity());
        for (int i = 0; i < mergedRowData.getArity(); i++) {
            if (rightType.getFieldTypes()[i].getTypeClass() != String.class) {
                mergedRowData.setField(i, null);
                continue;
            }

            StringBuilder columnValBuilder = new StringBuilder();
            for (int j = 0; j < rowDataList.size(); j++) {
                StringData colVal = rowDataList.get(j).getString(i);
                columnValBuilder.append(colVal == null ? "" : colVal.toString());
                if (j < rowDataList.size() - 1) {
                    columnValBuilder.append(joinMergeDelimiter);
                }
            }
            StringData columnVal = StringData.fromString(columnValBuilder.toString());
            mergedRowData.setField(i, columnVal);
        }
        return mergedRowData;
    }

    private long calExpirationTime(long operatorTime, long relativeSize) {
        if (operatorTime < Long.MAX_VALUE) {
            return operatorTime - relativeSize - allowedLateness - 1;
        } else {
            // When operatorTime = Long.MaxValue, it means the stream has reached the end.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Update the operator time of the two streams.
     * Must be the first call in all processing methods (i.e., processElement(), onTimer()).
     *
     * @param ctx the context to acquire watermarks
     */
    abstract void updateOperatorTime(Context ctx);

    /**
     * Return the time for the target row from the left stream.
     * Requires that [[updateOperatorTime()]] has been called before.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @return time for the target row
     */
    abstract long getTimeForLeftStream(Context ctx, RowData row);

    /**
     * Return the time for the target row from the right stream.
     * Requires that [[updateOperatorTime()]] has been called before.
     *
     * @param ctx the runtime context
     * @param row the target row
     * @return time for the target row
     */
    abstract long getTimeForRightStream(Context ctx, RowData row);

    /**
     * Register a proctime or rowtime timer.
     *
     * @param ctx         the context to register the timer
     * @param cleanupTime timestamp for the timer
     */
    abstract void registerTimer(Context ctx, long cleanupTime);
}
