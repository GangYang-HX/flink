package com.bilibili.bsql.hive.diversion.table;

import com.bilibili.bsql.hive.diversion.splitpolicy.MultiSinkSplitPolicy;
import com.bilibili.bsql.hive.diversion.table.assigner.MultiTableBucketAssigner;
import com.bilibili.bsql.hive.diversion.tableinfo.MultiHiveSinkTableInfo;
import com.bilibili.bsql.hive.filetype.orc.vector.RowVectorizer;
import com.bilibili.bsql.hive.filetype.orc.writer.OrcBulkWriterFactory;
import com.bilibili.bsql.hive.table.BsqlHiveConfigTableSinkFactory;
import com.bilibili.bsql.hive.table.assigner.BiliBucketAssigner;
import com.bilibili.bsql.hive.tableinfo.HiveSinkTableInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.bili.writer.sink.MultiStreamFileSystemTableSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.bilibili.bsql.hive.tableinfo.HiveConfig.*;
import static org.apache.flink.bili.writer.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/3 3:21 下午
 */
public class BsqlMultiHiveTableTableSinkFactory extends BsqlHiveConfigTableSinkFactory {

	public static final String IDENTIFIER = "bsql-hive-diversion";

	private static final Logger LOG = LoggerFactory.getLogger(BsqlMultiHiveTableTableSinkFactory.class);


	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, IDENTIFIER);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return Collections.singletonList("*");
	}

	@Override
	public TableSink<Row> createTableSink(Context context) {
		return getTableSink(new MultiHiveSinkTableInfo(context));
	}

	private TableSink<Row> getTableSink(MultiHiveSinkTableInfo multiHiveSinkTableInfo) {
		String[] location = multiHiveSinkTableInfo.getLocations();
		TableSink<Row> tableSink;
		HashMap<String, String> properties = getProperty(multiHiveSinkTableInfo);

		Properties writerProps = new Properties();
		if (StringUtils.isNotBlank(multiHiveSinkTableInfo.getCompress())) {
			writerProps.setProperty("orc.compress", multiHiveSinkTableInfo.getCompress());
		}

		DataType dataType = multiHiveSinkTableInfo.getDataType();
		RowType formatRowType = (RowType) dataType.getLogicalType();
		TypeDescription typeDescription =
			OrcSplitReaderUtil.logicalTypeToOrcType(formatRowType);


		Properties bufferProperties = new Properties();
		if (StringUtils.isNotBlank(multiHiveSinkTableInfo.getBufferSize())) {
			bufferProperties.put(BSQL_HIVE_BUFFER_SIZE.key(), Integer.parseInt(multiHiveSinkTableInfo.getBufferSize()));
		}
		if (StringUtils.isNotBlank(multiHiveSinkTableInfo.getBatchSize())) {
			bufferProperties.put(BSQL_HIVE_BATCH_SIZE.key(), Integer.parseInt(multiHiveSinkTableInfo.getBatchSize()));
		}
		OrcBulkWriterFactory<Row> writerFactory = new OrcBulkWriterFactory<>(
			new RowVectorizer(typeDescription.toString()),
			writerProps,
			new Configuration(),
			bufferProperties
		);
		tableSink = new MultiStreamFileSystemTableSink(new Path(StringUtils.substringBefore(location[0],
			new Path(location[0]).getPath()) + "/"),
			properties,
			multiHiveSinkTableInfo.getPartitionKeyList(),
			multiHiveSinkTableInfo.getTableSchema(),
			multiHiveSinkTableInfo.getObjectIdentifierMap())
			.withWriter(writerFactory)
			.withBucketAssigner(createBiliBucketAssigner(multiHiveSinkTableInfo))
			.withOutputFileConfig(OutputFileConfig.builder().build())
			.withTableMeta(multiHiveSinkTableInfo.getTableMeta())
			.withSplitPolicy(new MultiSinkSplitPolicy<>(properties, multiHiveSinkTableInfo.getTableMeta()
				, multiHiveSinkTableInfo.getTableTagUdfClassMode()
				, multiHiveSinkTableInfo.getTableTagFiledIndex(),
				multiHiveSinkTableInfo.getCustomSplitPolicyMap()));
		return tableSink;

	}

	@Override
	protected BiliBucketAssigner createBiliBucketAssigner(HiveSinkTableInfo hiveSinkTableInfo) {
		MultiHiveSinkTableInfo multiHiveSinkTableInfo = (MultiHiveSinkTableInfo) hiveSinkTableInfo;
		return new MultiTableBucketAssigner(multiHiveSinkTableInfo.getMultiPartitionUdfClass(),
			multiHiveSinkTableInfo.getMultiPartitionFieldIndex(),
			multiHiveSinkTableInfo.getPartitionUdf(),
			multiHiveSinkTableInfo.getPartitionUdfClass(),
			multiHiveSinkTableInfo.getPartitionKey(),
			multiHiveSinkTableInfo.getEventTimePos(),
			multiHiveSinkTableInfo.isAllowDrift(),
			multiHiveSinkTableInfo.isPathContainPartitionKey(), multiHiveSinkTableInfo.getTablePathMap());
	}

	@Override
	protected HashMap<String, String> getProperty(HiveSinkTableInfo sinkTableInfo) {
		HashMap<String, String> customProperties = super.getProperty(sinkTableInfo);
		customProperties.put(SINK_PARTITION_COMMIT_POLICY_KIND.key(), sinkTableInfo.getCommitPolicy() == null
			? "multi-metastore,custom" : sinkTableInfo.getCommitPolicy());
		return customProperties;
	}
}
