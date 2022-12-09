package com.bilibili.bsql.es.table;

import com.bilibili.bsql.es.format.EsOutputFormat;
import com.bilibili.bsql.es.tableinfo.EsSinkTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProviderWithParallel;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlEsDynamicSink.java
 * @description This is the description of BsqlEsDynamicSink.java
 * @createTime 2020-10-28 11:57:00
 */
public class BsqlEsDynamicSink implements DynamicTableSink, Serializable {

	private final EsSinkTableInfo sinkTableInfo;
	protected String[] fieldNames;
	private String address;
	private String indexName;
	private String typeName;
	private Class<?>[] bizFieldTypes;
	protected Integer batchMaxTimeout = 3000;
	protected Integer batchMaxCount = 1000;

	public BsqlEsDynamicSink(EsSinkTableInfo sinkTableInfo) {
		this.sinkTableInfo = sinkTableInfo;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return requestedMode;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		final SinkFunction<RowData> rowDataSinkFunction = createSinkFunction(sinkTableInfo, context);
		int sinkParallelism = sinkTableInfo.getParallelism();
		return SinkFunctionProviderWithParallel.of(rowDataSinkFunction, sinkParallelism);
	}

	private void initParams(EsSinkTableInfo sinkTableInfo) {
		this.batchMaxTimeout = sinkTableInfo.getBatchMaxTimeout() != null ? sinkTableInfo.getBatchMaxTimeout() : this.batchMaxTimeout;
		this.batchMaxCount = sinkTableInfo.getBatchMaxCount() != null ? sinkTableInfo.getBatchMaxCount() : this.batchMaxCount;
		this.address = sinkTableInfo.getAddress();
		this.indexName = sinkTableInfo.getIndexName();
		this.typeName = sinkTableInfo.getTypeName();
		this.bizFieldTypes = sinkTableInfo.getBizFieldClass();
		this.fieldNames = sinkTableInfo.getFieldNames();
	}

	public SinkFunction<RowData> createSinkFunction(EsSinkTableInfo sinkTableInfo, Context context) {
		initParams(sinkTableInfo);
		EsOutputFormat esOutputFormat = new EsOutputFormat(sinkTableInfo.getPhysicalRowDataType(), context);
		esOutputFormat.setBatchMaxCount(batchMaxCount);
		esOutputFormat.setBatchMaxTimeout(batchMaxTimeout);
		esOutputFormat.setAddress(address);
		esOutputFormat.setIndexName(indexName);
		if (StringUtils.isNotBlank(typeName)) {
			// esOutputFormat.setTypeName(typeName);默认logs，不允许用户设置
		}
		esOutputFormat.setFieldNames(fieldNames);
		esOutputFormat.setFieldTypes(bizFieldTypes);
		return esOutputFormat;
	}

	@Override
	public DynamicTableSink copy() {
		return new BsqlEsDynamicSink(sinkTableInfo);
	}

	@Override
	public String asSummaryString() {
		return "Es-Sink-" + sinkTableInfo.getName();
	}
}
