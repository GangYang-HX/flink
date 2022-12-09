package com.bilibili.bsql.hive.table;

import com.bilibili.bsql.hive.table.assigner.BiliBucketAssigner;
import com.bilibili.bsql.hive.tableinfo.HiveSinkTableInfo;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bilibili.bsql.hive.tableinfo.HiveConfig.BSQL_COMMIT_POLICY;
import static com.bilibili.bsql.hive.tableinfo.HiveConfig.BSQL_CUSTOM_POLICY_CLASS;
import static org.apache.flink.bili.writer.FileSystemOptions.*;
import static org.apache.flink.table.connector.ConnectorValues.DEFAULT_PARALLEL;

/**
 * @author: zhuzhengjun
 * @date: 2022/1/4 4:11 下午
 */
public class BsqlHiveConfigTableSinkFactory implements TableSinkFactory<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(BsqlHiveConfigTableSinkFactory.class);

	public HashMap<String, String> property = new HashMap<>();


	protected BiliBucketAssigner createBiliBucketAssigner(HiveSinkTableInfo hiveSinkTableInfo) {
		//do nothing;
		throw new UnsupportedOperationException("this factory can not determining the bucket assigner.");
	}

	protected HashMap<String, String> getProperty(HiveSinkTableInfo sinkTableInfo) {
		if (sinkTableInfo.getParallelism() != null) {
			property.put(SINK_WRITER_PARALLEL.key(), sinkTableInfo.getParallelism());
		} else {
			property.put(SINK_WRITER_PARALLEL.key(), DEFAULT_PARALLEL.toString());
		}
		property.put(SINK_PARTITION_COMMIT_POLICY_KIND.key(), sinkTableInfo.getCommitPolicy() == null
			? BSQL_COMMIT_POLICY.defaultValue() : sinkTableInfo.getCommitPolicy());
		property.put(SINK_PARTITION_COMMIT_POLICY_CLASS.key(), sinkTableInfo.getCustomPolicyReferences() == null ?
			BSQL_CUSTOM_POLICY_CLASS.defaultValue() : sinkTableInfo.getCustomPolicyReferences());
		if (sinkTableInfo.getEventTimePos() == -1 && sinkTableInfo.getMetaPos() == -1) {
			property.put(SINK_PARTITION_COMMIT_TRIGGER.key(), "process-part-time");
		} else {
			property.put(SINK_PARTITION_COMMIT_TRIGGER.key(), "partition-time");
		}
		property.put(SINK_PARTITION_COMMIT_DELAY.key(), sinkTableInfo.getCommitDelay());
		property.put(PARTITION_TIME_EXTRACTOR_KIND.key(), "custom");
		property.put(PARTITION_TIME_EXTRACTOR_CLASS.key(), "com.bilibili.bsql.hive.table.splitter.BsqlPartTimeExtractor");
		property.put(PARTITION_SPLIT_KIND.key(), "custom");
		property.put(PARTITION_SPLIT_CLASS.key(), "com.bilibili.bsql.hive.table.splitter.BsqlPathSplitter");
		property.put(SINK_HIVE_VERSION.key(), sinkTableInfo.getSinkHiveVersion());

		//owner id
		property.put(SYSTEM_USER_ID.key(), sinkTableInfo.getReadableConfig().get(SYSTEM_USER_ID));
		property.put(SINK_PARTITION_COMMIT_EAGERLY.key(), String.valueOf(sinkTableInfo.isEagerCommit()));
		property.put(SINK_PARTITION_ALLOW_DRIFT.key(), String.valueOf(sinkTableInfo.isAllowDrift()));
		property.put(SINK_PARTITION_ALLOW_EMPTY_COMMIT.key(), String.valueOf(sinkTableInfo.isAllowEmptyCommit()));
		property.put(SINK_PARTITION_PATH_CONTAIN_PARTITION_KEY.key(), String.valueOf(sinkTableInfo.isPathContainPartitionKey()));
		property.put(SINK_PARTITION_ENABLE_INDEX_FILE.key(), String.valueOf(sinkTableInfo.isEnableIndexFile()));
		property.put(SINK_PARTITION_TRACE_ID.key(), sinkTableInfo.getTraceId());
		property.put(SINK_PARTITION_TRACE_KIND.key(), sinkTableInfo.getTraceKind());
		property.put(SINK_PARTITION_CUSTOM_TRACE_CLASS.key(), sinkTableInfo.getCustomTraceClass());
		property.put(SINK_METASTORE_COMMIT_LIMIT_NUM.key(), sinkTableInfo.getMetastoreCommitLimit());
		property.put(SINK_DEFAULT_PARTITION_CHECK.key(), String.valueOf(sinkTableInfo.isDefaultPartitionCheck()));
		property.put(SINK_MULTI_PARTITION_EMPTY_COMMIT.key(), String.valueOf(sinkTableInfo.isMultiPartitionEmptyCommit()));
		property.put(SINK_PARTITION_META_POS.key(), sinkTableInfo.getMetaPos().toString());
		property.put(SINK_PARTITION_EMPTY_SWITCH_TIME.key(), sinkTableInfo.getSwitchTime().toString());
		property.put(SINK_PARTITION_ROLLBACK.key(), String.valueOf(sinkTableInfo.isPartitionRollback()));
		property.put(SINK_HIVE_TABLE_COMPRESS.key(), sinkTableInfo.getCompress());
		property.put(SINK_ROLLING_POLICY_FILE_SIZE.key(), String.valueOf(sinkTableInfo.getRollingPolicyFileSize()));
		property.put(SINK_ROLLING_POLICY_TIME_INTERVAL.key(), String.valueOf(sinkTableInfo.getRollingPolicyTimeInterval()));
		LOG.info("StreamFileSystemTableSink properties : " + property);
		return property;
	}

	@Override
	public Map<String, String> requiredContext() {
		throw new UnsupportedOperationException("BsqlHiveConfigContext not supported requiredContext yet.");
	}

	@Override
	public List<String> supportedProperties() {
		throw new UnsupportedOperationException("BsqlHiveConfigContext not supported supportedProperties yet.");
	}

	@Override
	public TableSink<Row> createTableSink(Context context) {
		throw new UnsupportedOperationException("BsqlHiveConfigContext not supported createTableSink yet.");
	}

}
