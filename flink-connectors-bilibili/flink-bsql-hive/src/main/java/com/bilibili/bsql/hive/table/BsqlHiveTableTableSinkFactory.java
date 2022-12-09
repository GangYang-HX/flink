package com.bilibili.bsql.hive.table;
import com.bilibili.bsql.hive.table.assigner.BiliBucketAssigner;
import com.bilibili.bsql.hive.table.assigner.LancerBucketAssigner;
import com.bilibili.bsql.hive.table.assigner.UdfBucketAssigner;
import com.bilibili.bsql.hive.tableinfo.HiveSinkTableInfo;
import org.apache.flink.bili.writer.sink.NewStreamFileSystemTableSink;
import org.apache.flink.bili.writer.sink.StreamFileSystemTableSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import com.bilibili.bsql.hive.filetype.compress.CompressEnum;
import com.bilibili.bsql.hive.filetype.compress.CompressWriterFactory;
import com.bilibili.bsql.hive.filetype.compress.writer.extractor.DefaultExtractor;
import com.bilibili.bsql.hive.filetype.orc.vector.RowVectorizer;
import com.bilibili.bsql.hive.filetype.orc.writer.OrcBulkWriterFactory;
import com.bilibili.bsql.hive.filetype.parquet.SchemaUtil;
import com.bilibili.bsql.hive.filetype.parquet.writer.BiliParquetAvroWriters;
import com.bilibili.bsql.hive.filetype.text.BiliSimpleStringEncoder;
import com.bilibili.bsql.hive.tableinfo.SingleHiveSinkTableTableInfo;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.bilibili.bsql.hive.tableinfo.HiveConfig.*;
import static org.apache.flink.bili.writer.FileSystemOptions.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlHiveTableSinkFactory.java
 * @description This is the description of BsqlHiveTableSinkFactory.java
 * @createTime 2020-11-09 12:28:00
 */
public class BsqlHiveTableTableSinkFactory extends BsqlHiveConfigTableSinkFactory {
	private static final Logger LOG = LoggerFactory.getLogger(BsqlHiveTableTableSinkFactory.class);
	public static final String IDENTIFIER = "bsql-hive";



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
		return getTableSink(new SingleHiveSinkTableTableInfo(context));
	}

	private TableSink<Row> getTableSink(SingleHiveSinkTableTableInfo sinkTableInfo) {

		LOG.info("StreamFileSystemTableSink format : {},compress : {} ", sinkTableInfo.getFormat(), sinkTableInfo.getCompress());

		TableSink<Row> tableSink;
		switch (sinkTableInfo.getFormat()) {
			case "orc":
				tableSink = new NewStreamFileSystemTableSink(
						new Path(sinkTableInfo.getLocation()),
						getProperty(sinkTableInfo),
						sinkTableInfo.getPartitionKeyList(),
						sinkTableInfo.getTableSchema(),
						sinkTableInfo.getObjectIdentifier())
						.withWriter(createWriter(sinkTableInfo))
						.withBucketAssigner(createBiliBucketAssigner(sinkTableInfo))
						.withOutputFileConfig(buildOutputFileConfig(sinkTableInfo));
				break;

			default:
				tableSink = new StreamFileSystemTableSink(
						new Path(sinkTableInfo.getLocation()),
						getProperty(sinkTableInfo),
						sinkTableInfo.getPartitionKeyList(),
						sinkTableInfo.getObjectIdentifier())
						.withWriter(createWriter(sinkTableInfo))
						.withBucketAssigner(createBiliBucketAssigner(sinkTableInfo))
						.withOutputFileConfig(buildOutputFileConfig(sinkTableInfo))
						.configure(sinkTableInfo.getFieldNames(), sinkTableInfo.getTypeInformations());
				break;


		}
		return tableSink;
	}

	protected Object createWriter(HiveSinkTableInfo hiveSinkTableInfo) {
		SingleHiveSinkTableTableInfo singleHiveSinkTableInfo = (SingleHiveSinkTableTableInfo) hiveSinkTableInfo;
		String[] fieldNames = singleHiveSinkTableInfo.getFieldNames();
		Class<?>[] fieldClassName = singleHiveSinkTableInfo.getFieldClass();

		switch (singleHiveSinkTableInfo.getFormat()) {
			case "orc":
				Properties writerProps = new Properties();
				if (StringUtils.isNotBlank(singleHiveSinkTableInfo.getCompress())) {
					writerProps.setProperty("orc.compress", singleHiveSinkTableInfo.getCompress());
				}

				DataType dataType = singleHiveSinkTableInfo.getDataType();
				RowType formatRowType = (RowType) dataType.getLogicalType();

				TypeDescription typeDescription = OrcSplitReaderUtil.constructRowType(formatRowType, singleHiveSinkTableInfo.getMetaPos());

				Properties bufferProperties = new Properties();
				if (StringUtils.isNotBlank(singleHiveSinkTableInfo.getBufferSize())) {
					bufferProperties.put(BSQL_HIVE_BUFFER_SIZE.key(), Integer.parseInt(singleHiveSinkTableInfo.getBufferSize()));
				}
				if (StringUtils.isNotBlank(singleHiveSinkTableInfo.getBatchSize())) {
					bufferProperties.put(BSQL_HIVE_BATCH_SIZE.key(), Integer.parseInt(singleHiveSinkTableInfo.getBatchSize()));
				}
				if (StringUtils.isNotBlank(singleHiveSinkTableInfo.getOrcRowsBetweenMemoryChecks())) {
					bufferProperties.put(BSQL_ORC_ROWS_BETWEEN_MEMORY_CHECKS.key(), singleHiveSinkTableInfo.getOrcRowsBetweenMemoryChecks());
				}

				return new OrcBulkWriterFactory<>(
						new RowVectorizer(typeDescription.toString(), singleHiveSinkTableInfo.getMetaPos()),
						writerProps,
						new Configuration(),
						bufferProperties
				);

			case "parquet":
				return BiliParquetAvroWriters.forRow(
						SchemaUtil.convertSchema(fieldNames, fieldClassName).toString(),
						CompressionCodecName.UNCOMPRESSED);

			default:
				CompressEnum compressEnum = CompressEnum.getCompressEnumByName(singleHiveSinkTableInfo.getCompress());
				if (null != compressEnum) {
					return new CompressWriterFactory(
							new DefaultExtractor(
									singleHiveSinkTableInfo.getFieldDelim(),
									singleHiveSinkTableInfo.getEventTimePos(),
									singleHiveSinkTableInfo.isTimeFieldVirtual()),
							singleHiveSinkTableInfo.getRowDelim())
							.build(compressEnum.getCompression(), new Configuration());
				} else {
					return new BiliSimpleStringEncoder(
							singleHiveSinkTableInfo.getFieldDelim(),
							singleHiveSinkTableInfo.getRowDelim(),
							singleHiveSinkTableInfo.getEventTimePos(),
							singleHiveSinkTableInfo.isTimeFieldVirtual());
				}
		}
	}

	protected OutputFileConfig buildOutputFileConfig(HiveSinkTableInfo hiveSinkTableInfo){
		CompressEnum compressEnum = CompressEnum.getCompressEnumByName(hiveSinkTableInfo.getCompress());
		String partSuffix = compressEnum == null ? "" : compressEnum.getCompressFileSuffix();
		return OutputFileConfig.builder().useTimestampFilePrefix(hiveSinkTableInfo.isUseTimestampFilePrefix())
				.withPartSuffix(partSuffix).build();
	}

	@Override
	protected BiliBucketAssigner createBiliBucketAssigner(HiveSinkTableInfo hiveSinkTableInfo) {
		if (hiveSinkTableInfo.useMultiPartitionUdf() && hiveSinkTableInfo instanceof SingleHiveSinkTableTableInfo) {
			SingleHiveSinkTableTableInfo singleHiveSinkTableInfo = (SingleHiveSinkTableTableInfo) hiveSinkTableInfo;
			return new UdfBucketAssigner(
				singleHiveSinkTableInfo.getMultiPartitionUdfClass(),
				singleHiveSinkTableInfo.getMultiPartitionFieldIndex(),
				singleHiveSinkTableInfo.getPartitionUdf(),
				singleHiveSinkTableInfo.getPartitionUdfClass(),
				singleHiveSinkTableInfo.getPartitionKey(),
				singleHiveSinkTableInfo.getEventTimePos(),
				singleHiveSinkTableInfo.isAllowDrift(),
				singleHiveSinkTableInfo.isPathContainPartitionKey()
			);
		}  else if (hiveSinkTableInfo.getMetaPos() != -1){
			SingleHiveSinkTableTableInfo singleHiveSinkTableInfo = (SingleHiveSinkTableTableInfo) hiveSinkTableInfo;
			return new LancerBucketAssigner(
					singleHiveSinkTableInfo.getPartitionKey(),
					singleHiveSinkTableInfo.getEventTimePos(),
					singleHiveSinkTableInfo.getMetaPos(),
					singleHiveSinkTableInfo.getBucketTimeKey(),
					singleHiveSinkTableInfo.isAllowDrift(),
					singleHiveSinkTableInfo.isPathContainPartitionKey()
			);
		} else {
			SingleHiveSinkTableTableInfo singleHiveSinkTableInfo = (SingleHiveSinkTableTableInfo) hiveSinkTableInfo;
			return new BiliBucketAssigner(
				singleHiveSinkTableInfo.getPartitionKey(),
				singleHiveSinkTableInfo.getEventTimePos(),
				singleHiveSinkTableInfo.isAllowDrift(),
				singleHiveSinkTableInfo.isPathContainPartitionKey()
			);
		}
	}

	@Override
	protected HashMap<String, String> getProperty(HiveSinkTableInfo sinkTableInfo) {
		super.getProperty(sinkTableInfo);
		SingleHiveSinkTableTableInfo singleHiveSinkTableInfo = (SingleHiveSinkTableTableInfo) sinkTableInfo;
		String[] tableName = singleHiveSinkTableInfo.getTableName().split("\\.");
		property.put(SINK_HIVE_DATABASE.key(), tableName[0]);
		property.put(SINK_HIVE_TABLE.key(), tableName[1]);
		property.put(SINK_HIVE_TABLE_FORMAT.key(), singleHiveSinkTableInfo.getFormat());
		return property;
	}
}
