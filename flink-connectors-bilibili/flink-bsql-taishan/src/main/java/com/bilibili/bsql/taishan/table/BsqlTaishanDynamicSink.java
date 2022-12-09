package com.bilibili.bsql.taishan.table;

import com.bilibili.bsql.taishan.format.TaishanOutputFormat;
import com.bilibili.bsql.taishan.tableinfo.TaishanSinkTableInfo;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProviderWithParallel;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhuzhengjun
 * @date 2020/11/2 12:14 下午
 */
public class BsqlTaishanDynamicSink implements DynamicTableSink {

	private static final Logger LOG = LoggerFactory.getLogger(BsqlTaishanDynamicSink.class);

    private TaishanSinkTableInfo tableInfo;

    public BsqlTaishanDynamicSink(TaishanSinkTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType rowDataType = tableInfo.getPhysicalRowDataType();
        Integer batchMaxTimeout = tableInfo.getBatchMaxTimeout()!= null ? tableInfo.getBatchMaxTimeout() : 0;
        Integer batchMaxCount = tableInfo.getBatchMaxCount() != null ? tableInfo.getBatchMaxCount() : 0;
        TaishanOutputFormat taishanOutputFormat = new TaishanOutputFormat(
			rowDataType,
			context,
			tableInfo.getCluster(),
			tableInfo.getZone(),
			tableInfo.getTtl(),
			tableInfo.getTable(),
			tableInfo.getPassword(),
			tableInfo.getFieldNames(),
			tableInfo.getKeyIndex(),
			tableInfo.getFieldIndex(),
			tableInfo.getValueIndex(),
			tableInfo.getRetryCount(),
			tableInfo.getRetryInterval(),
			tableInfo.getDuration(),
			tableInfo.getKeepAliveTime(),
			tableInfo.getKeepAliveTimeout(),
			tableInfo.getIdleTimeout()
		);
        taishanOutputFormat.setBatchMaxCount(batchMaxCount);
        taishanOutputFormat.setBatchMaxTimeout(batchMaxTimeout);
		taishanOutputFormat.setFieldTypes(rowDataType);
		LOG.info("taishan output config :{}.", taishanOutputFormat.toString());
        Integer parellel = tableInfo.getParallelism();
        return SinkFunctionProviderWithParallel.of(taishanOutputFormat,parellel);
    }

    @Override
    public DynamicTableSink copy() {
        return new BsqlTaishanDynamicSink(tableInfo);
    }

    @Override
    public String asSummaryString() {
        return "Taishan table sink";
    }

}
