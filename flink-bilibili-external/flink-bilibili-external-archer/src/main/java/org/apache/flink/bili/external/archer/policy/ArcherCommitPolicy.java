package org.apache.flink.bili.external.archer.policy;

import avro.shaded.com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.bili.external.archer.ArcherOperator;
import org.apache.flink.bili.external.archer.constant.ArcherConstants;
import org.apache.flink.bili.writer.StreamingFileCommitter;
import org.apache.flink.bili.writer.commitpolicy.PartitionCommitPolicy;
import org.apache.flink.bili.writer.exception.PartitionNotReadyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author: zhuzhengjun
 * @date: 2021/12/22 4:38 下午
 */
public class ArcherCommitPolicy implements PartitionCommitPolicy {
	private static final Logger LOG = LoggerFactory.getLogger(ArcherCommitPolicy.class);

	private ArcherOperator archerOperator = null;

	private Properties properties;


	@Override
	public void commit(Context context) throws Exception {
		if (archerOperator == null) {
			init(context);
		}

		if (!context.isAllPartitionsReady()) {
			throw new PartitionNotReadyException("open do not equals committed partitions");
		}
		LOG.info("Archer commit start, {}: {}", context.getClass().getSimpleName(), context);
		String database = context.databaseName();
		String table = context.tableName();
		String logId = "";
		if (context instanceof StreamingFileCommitter.PolicyContext) {
			logId = ((StreamingFileCommitter.PolicyContext) context).logId();
		}
		List<String> values = context.partitionValues();
		List<String> keys = context.partitionKeys();
		if (StringUtils.isNotBlank(logId)) {
			archerOperator.updateInstanceStatusByRelateUID(logId, keys, values);
		}else {
			boolean archerCommit = archerOperator.updatePartitionStatus(database, table, keys, values);
			if (!archerCommit) {
				this.archerOperator = null;
				throw new RuntimeException
					("archer commit failed,will try again later, database: " + database + " table: " + table);
			}
		}

		LOG.info("{} process {} context", this.getClass().getSimpleName(), context);
		// in order to support multi hive sink table
		// this will be null next time
		this.archerOperator = null;
	}

	private void init(Context context) {
		Map<String, String> config = Maps.newHashMap();
		config.put(ArcherConstants.SINK_DATABASE_NAME_KEY, context.databaseName());
		config.put(ArcherConstants.SINK_TABLE_NAME_KEY, context.tableName());
		config.put(ArcherConstants.SYSTEM_USER_ID, context.owner());
		config.put(ArcherConstants.SINK_PARTITION_KEY, StringUtils.join(context.partitionKeys(), ","));
		String logId = "";
		if (context instanceof StreamingFileCommitter.PolicyContext) {
			logId = ((StreamingFileCommitter.PolicyContext) context).logId();
		}
		config.put(ArcherConstants.LANCER_LOG_ID, logId);
		properties = MapUtils.toProperties(config);
		LOG.info("properties = {}", properties);
		this.archerOperator = new ArcherOperator(properties);
		LOG.info("create archer commit helper successful");
		if (StringUtils.isBlank(logId)) {
			archerOperator.open();
		}
	}
}
