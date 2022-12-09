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
package org.apache.flink.bili.writer.commitpolicy;

import java.util.LinkedHashMap;
import java.util.Optional;

import org.apache.flink.trace.Trace;
import org.apache.flink.trace.TraceWrapper;
import org.apache.flink.bili.writer.metastorefactory.TableMetaStoreFactory;
import org.apache.flink.bili.writer.metricsetter.TriggerMetricsWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Partition commit policy to update metastore.
 *
 * <p>If this is for file system table, the metastore is a empty implemantation.
 * If this is for hive table, the metastore is for connecting to hive metastore.
 */
public class MetastoreCommitPolicy implements PartitionCommitPolicy, TraceWrapper {

	private static final Logger LOG = LoggerFactory.getLogger(MetastoreCommitPolicy.class);
	private TableMetaStoreFactory.TableMetaStore metaStore;
	private TriggerMetricsWrapper triggerMetricsWrapper;
	private Trace trace;

	public void setMetastore(TableMetaStoreFactory.TableMetaStore metaStore) {
		this.metaStore = metaStore;
	}

	public void setTriggerMetricsWrapper(TriggerMetricsWrapper triggerMetricsWrapper) {
		this.triggerMetricsWrapper = triggerMetricsWrapper;
	}

	@Override
	public void commit(Context context) throws Exception {
		LOG.info("metastore commit before executed;context.partitionSpec:{}",context.partitionSpec());
		metaStore.createIfNotExistPartition(context.partitionSpec(), context.partitionPath(), this.metaStore, triggerMetricsWrapper, trace, context.eagerCommit());
	}

	public boolean checkPartitionSize(int index, int limit) throws IllegalArgumentException {
		if (index > limit) {
			throw new IllegalArgumentException("hive metastore create too many partitions，limit " + limit + " once.");
		}
		return true;
	}

	@Override
	public void setTrace(Trace trace) {
		this.trace = trace;
	}
}
