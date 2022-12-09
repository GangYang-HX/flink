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

package com.bilibili.bsql.redis.sink;

import com.bilibili.bsql.redis.tableinfo.RedisSinkTableInfo;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProviderWithParallel;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;


/**
 *
 *
 */
@Internal
public class BsqlRedisDynamicSink implements DynamicTableSink {

	private RedisSinkTableInfo tableInfo;

	public BsqlRedisDynamicSink(RedisSinkTableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (RowKind kind : requestedMode.getContainedKinds()) {
			if (kind != RowKind.UPDATE_BEFORE) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

		DataType consumedDataType = tableInfo.getPhysicalRowDataType();
		DataStructureConverter converter = context.createDataStructureConverter(consumedDataType);

		Integer batchMaxTimeout = tableInfo.getBatchMaxTimeout()!= null ? tableInfo.getBatchMaxTimeout() : 0;
		Integer batchMaxCount = tableInfo.getBatchMaxCount() != null ? tableInfo.getBatchMaxCount() : 0;

        RedisOutputFormat redisOutputFormat = new RedisOutputFormat(consumedDataType,context);
        redisOutputFormat.setUrl(tableInfo.getUrl());
        redisOutputFormat.setKeyIndex(tableInfo.getKeyIndex());
        redisOutputFormat.setValIndex(tableInfo.getValueIndex());
        redisOutputFormat.setFieldIndex(tableInfo.getFieldIndex());
        redisOutputFormat.setFieldTypes(consumedDataType);
        redisOutputFormat.setPrimaryKeys(tableInfo.getPrimaryKeys());
        redisOutputFormat.setTimeout(tableInfo.getTimeout());
        redisOutputFormat.setRedisType(tableInfo.getRedisType());
        redisOutputFormat.setMaxTotal(tableInfo.getMaxTotal());
        redisOutputFormat.setMaxIdle(tableInfo.getMaxIdle());
        redisOutputFormat.setMinIdle(tableInfo.getMinIdle());
        redisOutputFormat.setExpire(tableInfo.getExpire());
        redisOutputFormat.setConverter(converter);
        redisOutputFormat.setBatchMaxCount(batchMaxCount);
        redisOutputFormat.setBatchMaxTimeout(batchMaxTimeout);

		return SinkFunctionProviderWithParallel.of(redisOutputFormat, tableInfo.getParallelism());
	}

	@Override
	public DynamicTableSink copy() {
		return new BsqlRedisDynamicSink(tableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Redis table sink";
	}
}
