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

package com.bilibili.bsql.hbase.sink;

import java.time.Duration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProviderWithParallel;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import com.bilibili.bsql.hbase.tableinfo.HbaseSinkTableInfo;

import static java.time.temporal.ChronoUnit.MILLIS;


/**
 *
 */
@Internal
public class BsqlHbaseDynamicSink implements DynamicTableSink {

	private HbaseSinkTableInfo tableInfo;

	public BsqlHbaseDynamicSink(HbaseSinkTableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		// UPSERT mode
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

		Configuration hbaseClientConf = HBaseConfiguration.create();
		hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, tableInfo.getHost());
		hbaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, tableInfo.getParent());

		String tableName = tableInfo.getTableName();

		RowDataToMutationConverter converter = new RowDataToMutationConverter(
				tableInfo.getHBaseTableSchema(),
				"null",
				tableInfo.isSkipWal(),
				tableInfo.getRowIdx(),
				tableInfo.getColIdx(),
				tableInfo.getVersionDataType());

		Duration flushInterval = Duration.of(tableInfo.getBatchMaxTimeout()==null ?
				60000 : tableInfo.getBatchMaxTimeout(),
				MILLIS);
		Integer flushRows = tableInfo.getBatchMaxCount() == null ? 1000 : tableInfo.getBatchMaxCount();
		Integer flushSize = 0;

		HBaseSinkFunction<RowData> sinkFunction = new HBaseSinkFunction<>(
				tableName,
				hbaseClientConf,
				converter,
				flushSize,
				flushRows,
				flushInterval.toMillis());

		return SinkFunctionProviderWithParallel.of(sinkFunction, tableInfo.getParallelism());
	}

	@Override
	public DynamicTableSink copy() {
		return new BsqlHbaseDynamicSink(tableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Hbase table sink";
	}
}
