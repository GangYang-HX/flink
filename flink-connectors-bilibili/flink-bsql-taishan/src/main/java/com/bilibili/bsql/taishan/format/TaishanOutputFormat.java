package com.bilibili.bsql.taishan.format;

import com.bapis.infra.service.taishan.*;
import com.bilibili.bsql.common.format.BatchOutputFormat;
import com.google.protobuf.ByteString;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author zhuzhengjun
 * @date 2020/10/29 11:45 上午
 */

public class TaishanOutputFormat extends BatchOutputFormat {
    private final static Logger LOG = LoggerFactory.getLogger(TaishanOutputFormat.class);
    private final String cluster;
    private final String zone;
    private final int ttl;
    private final String table;
    private final String password;
    private int taskNumber;
    private final String[] fieldNames;
    private TaishanRPC taiShanRPC;
    private BatchPutReq.Builder batchPutReqBuilder;
    private final DynamicTableSink.DataStructureConverter converter;
    protected DataType fieldTypes;
    private final int keyIndex;
    private final int fieldIndex;
    private final int valIndex;
	private final int retryCount;
	private final int retryInterval;
	private final int duration;
	private final int keepAliveTime;
	private final int keepAliveTimeout;
	private final int idleTimeout;

    @Override
    public String sinkType() {
        return "taishan";
    }

    public TaishanOutputFormat(DataType dataType,
							   DynamicTableSink.Context context,
							   String cluster,
							   String zone,
							   int ttl,
							   String table,
							   String password,
							   String[] fieldNames,
							   int keyIndex,
							   int fieldIndex,
							   int valIndex,
							   int retryCount,
							   int retryInterval,
							   int duration,
							   int keepAliveTime,
							   int keepAliveTimeout,
							   int idleTimeout) {
        super(dataType,context);
        this.converter = context.createDataStructureConverter(dataType);
		this.cluster = cluster;
		this.zone = zone;
		this.ttl = ttl;
		this.table = table;
		this.password = password;
		this.fieldNames = fieldNames;
		this.keyIndex = keyIndex;
		this.fieldIndex = fieldIndex;
		this.valIndex = valIndex;
		this.retryCount = retryCount;
		this.retryInterval = retryInterval;
		this.duration = duration;
		this.keepAliveTime = keepAliveTime;
		this.keepAliveTimeout = keepAliveTimeout;
		this.idleTimeout = idleTimeout;
    }

    @Override
    public void doOpen(int taskNumber, int numTasks) throws IOException {
        super.doOpen(taskNumber, numTasks);
        this.taskNumber = taskNumber;
		taiShanRPC = new TaishanRPC(zone, cluster, keepAliveTime, keepAliveTimeout, idleTimeout);
        batchPutReqBuilder = BatchPutReq.newBuilder();
    }

    @Override
    protected void doWrite(RowData record) {
        if (record.getRowKind() == RowKind.UPDATE_BEFORE || record.getRowKind() == RowKind.DELETE) {
            return;
        }

        Row row = (Row) converter.toExternal(record);

        if (row == null || row.getArity() == 0) {
            return;
        }
        if (row.getArity() != ((RowType) fieldTypes.getLogicalType()).getFieldCount()) {
            return;
        }
        Object key = row.getField(keyIndex);
        Object field = null;
        if (fieldIndex != -1) {
            field = row.getField(fieldIndex);
        }
        Object value = row.getField(valIndex);
        Record.Builder recordBuilder = Record.newBuilder();
        recordBuilder.setKey(ByteString.copyFromUtf8(String.valueOf(key)));
        Column.Builder columnBuilder = Column.newBuilder();
        columnBuilder.setName(ByteString.copyFromUtf8(String.valueOf(key)));
        columnBuilder.setValue(ByteString.copyFromUtf8(String.valueOf(value)));
        if (ttl > 0) {
            recordBuilder.setTtl(ttl);
        }
        recordBuilder.addColumns(columnBuilder.build());
        batchPutReqBuilder.addRecords(recordBuilder.build());

    }

    @Override
    protected void flush() {
        batchPutReqBuilder.setTable(table);
        Auth.Builder authBuilder = Auth.newBuilder();
        authBuilder.setToken(password);
        batchPutReqBuilder.setAuth(authBuilder.build());
		int retryTimes = this.retryCount;
		BatchPutResp resp;
		if (batchPutReqBuilder.getRecordsCount() > 0) {
			resp = taiShanRPC.batchPut(batchPutReqBuilder.build(), duration);
		}else {
			LOG.debug("Batch put record size = {}, not need to flush.", batchPutReqBuilder.getRecordsCount());
			return;
		}
		while (!resp.getAllSucceed() && retryTimes > 0) {
			try {
				Thread.sleep(retryInterval);
			} catch (InterruptedException e) {
				LOG.error("taishan sink flush thread with interruptedException :", e);
			}
			resp = taiShanRPC.batchPut(batchPutReqBuilder.build(), duration);
			retryTimes--;
		}
		if (!resp.getAllSucceed()) {
			if (resp.getRecordsCount() > 0) {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < resp.getRecordsCount(); i++) {
					sb.append(resp.getRecords(i).getStatus().getMsg()).append(";");
				}
				throw new RuntimeException("taishan sink flush failed after " +
					this.retryCount + " times. cause all status like " + sb.toString());
			} else {
				LOG.error("Can not put all batch and resp record size is {}, retry times:{}.", resp.getRecordsCount(),
					this.retryCount);
			}
		}
		batchPutReqBuilder.clear();
		LOG.debug("taskNum:{}, {} retry times left, flush success...", taskNumber, retryTimes);

    }

    @Override
    public void close() throws IOException {
        super.close();
        if (taiShanRPC != null) {
            taiShanRPC.close();
        }
    }

    public TaishanRPC getTaiShanRPC() {
        return taiShanRPC;
    }

    public DataType getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(DataType fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

	@Override
	public String toString() {
		return "TaishanOutputFormat{" +
			"cluster='" + cluster + '\'' +
			", zone='" + zone + '\'' +
			", ttl=" + ttl +
			", table='" + table + '\'' +
			", password='" + password + '\'' +
			", taskNumber=" + taskNumber +
			", fieldNames=" + Arrays.toString(fieldNames) +
			", taiShanRPC=" + taiShanRPC +
			", batchPutReqBuilder=" + batchPutReqBuilder +
			", converter=" + converter +
			", fieldTypes=" + fieldTypes +
			", keyIndex=" + keyIndex +
			", fieldIndex=" + fieldIndex +
			", valIndex=" + valIndex +
			", retryCount=" + retryCount +
			", retryInterval=" + retryInterval +
			", duration=" + duration +
			", keepAliveTime=" + keepAliveTime +
			", keepAliveTimeout=" + keepAliveTimeout +
			", idleTimeout=" + idleTimeout +
			'}';
	}
}
