package com.bilibili.bsql.databus.table;

import com.bilibili.bsql.databus.function.BsqlDataBusSourceFunction;
import com.bilibili.bsql.databus.tableinfo.BsqlDataBusConfig;
import com.bilibili.bsql.databus.tableinfo.DataBusSourceTableInfo;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProviderWithExternalConfig;
import org.apache.flink.table.connector.source.SourceFunctionProviderWithParallel;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlDataBusDynamicSource.java
 * @description This is the description of BsqlDataBusDynamicSource.java
 * @createTime 2020-10-22 18:22:00
 */
@Slf4j
public class BsqlDataBusDynamicSource implements ScanTableSource {

	private final DataBusSourceTableInfo sourceTableInfo;
	protected final DataType outputDataType;
	protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	public BsqlDataBusDynamicSource(DataBusSourceTableInfo sourceTableInfo,
									DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
									DataType outputDataType) {
		this.sourceTableInfo = sourceTableInfo;
		this.decodingFormat = decodingFormat;
		this.outputDataType = outputDataType;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> deserializationSchema = this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.outputDataType);
		int sourceParallel = sourceTableInfo.getParallelism();
		if (sourceTableInfo.getPartitionNum() == null) {
			log.info("databus source no source partition num, use global parallel");
			return SourceFunctionProviderWithParallel.of(
				new BsqlDataBusSourceFunction<>(buildProperties(sourceTableInfo), deserializationSchema),
				false,
				sourceParallel
			);
		} else {
			log.info("databus source partition num:{}, sourceParallel:{}",sourceTableInfo.getPartitionNum(), sourceParallel);
			return SourceFunctionProviderWithExternalConfig.of(
				new BsqlDataBusSourceFunction<>(buildProperties(sourceTableInfo), deserializationSchema),
				false,
				sourceParallel,
				sourceTableInfo.getPartitionNum()
			);
		}

	}

	private Properties buildProperties(DataBusSourceTableInfo sourceTableInfo) {
		Properties properties = new Properties();
		properties.put(BsqlDataBusConfig.APPKEY_KEY, sourceTableInfo.getAppKey());
		properties.put(BsqlDataBusConfig.APPSECRET_KEY, sourceTableInfo.getAppSecret());
		properties.put(BsqlDataBusConfig.GROUPID_KEY, sourceTableInfo.getGroupId());
		properties.put(BsqlDataBusConfig.TOPIC_KEY, sourceTableInfo.getTopic());
		properties.put(BsqlDataBusConfig.ADDRESS_KEY, sourceTableInfo.getAddress());
		if (sourceTableInfo.getPartitionNum() != null) {
			properties.put(BsqlDataBusConfig.PARTITION, sourceTableInfo.getPartitionNum());
		}
		return properties;
	}

	@Override
	public DynamicTableSource copy() {
		return new BsqlDataBusDynamicSource(sourceTableInfo, decodingFormat, outputDataType);
	}

	@Override
	public String asSummaryString() {
		return "DataBus-Source-" + sourceTableInfo.getName();
	}
}
