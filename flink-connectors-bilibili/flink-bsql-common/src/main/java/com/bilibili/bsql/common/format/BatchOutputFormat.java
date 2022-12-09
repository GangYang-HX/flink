package com.bilibili.bsql.common.format;

import com.bilibili.bsql.common.metrics.CustomizeRichSinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BatchOutputFormat.java
 * @description This is the description of BatchOutputFormat.java
 * @createTime 2020-10-20 15:25:00
 */
public abstract class BatchOutputFormat extends CustomizeRichSinkFunction<RowData> implements CheckpointedFunction {
	private final static Logger LOG = LoggerFactory.getLogger(BatchOutputFormat.class);
	protected ScheduledExecutorService scheduledExecutorService;
	protected Integer batchMaxTimeout;
	protected Integer batchMaxCount;
	private long curCount = 0;
	protected int taskNumber;
	protected final Lock writeLock = new ReentrantLock();

    private final TypeInformation<RowData> typeInformation;
	private TypeSerializer<RowData> typeSerializer;
	/** Whether object reuse has been enabled or disabled. */
	private transient boolean isObjectReuseEnabled;


	public BatchOutputFormat(DataType dataType, DynamicTableSink.Context context){
        this.typeInformation = (TypeInformation<RowData>) context.createTypeInformation(dataType);
    }

	public void doOpen(int taskNumber, int numTasks) throws IOException {
        this.typeSerializer = typeInformation.createSerializer(getRuntimeContext().getExecutionConfig());
		this.isObjectReuseEnabled = getRuntimeContext().getExecutionConfig().isObjectReuseEnabled();
		this.taskNumber = taskNumber;
		startFlushScheduler();
	}

	@Override
	public boolean doInvoke(RowData value, Context context) throws Exception {
		return writeRecord(value);
	}

	public boolean writeRecord(RowData rowData) throws Exception {
		try {
			writeLock.lock();
			RowData record;
			if (isObjectReuseEnabled) {
				record = typeSerializer.copy(rowData);
			} else {
				record = rowData;
			}
			doWrite(record);
			curCount++;
			if (batchMaxCount != null && batchMaxCount > 0 && curCount >= batchMaxCount) {
				long start = System.nanoTime();
				flush();
				sinkMetricsGroup.rtFlushRecord(start);
				curCount = 0;
			}
		} catch (Exception e) {
			LOG.error("write data error for record: {}", rowData, e);
			// for exactly-once, we must throw exception.
			throw e;
		} finally {
			writeLock.unlock();
		}
		return true;
	}

	private void startFlushScheduler() {
		if (null != scheduledExecutorService) {
			return;
		}
		if (batchMaxTimeout != null && batchMaxTimeout > 0) {
			scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
			scheduledExecutorService.scheduleWithFixedDelay(() -> {
				try {
					if (needValidateTest()) {
						validateTest();
					}
					writeLock.lock();
					if (curCount > 0) {
						flush();
					}
					curCount = 0;
				} catch (Exception e) {
					LOG.error("flush error,shutdown this scheduler", e);
					scheduledExecutorService.shutdown();
				} finally {
					writeLock.unlock();
				}
			}, batchMaxTimeout, batchMaxTimeout, TimeUnit.MILLISECONDS);
		}
	}

	protected abstract void doWrite(RowData record);

	/**
	 * flush
	 *
	 * @throws Exception
	 */
	protected abstract void flush() throws Exception;

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		try {
			writeLock.lock();
			long start = System.nanoTime();
			flush();
			sinkMetricsGroup.rtFlushRecord(start);
			curCount = 0;
			LOG.info("batch output format checkpoint execute success!");
		} catch (Exception e) {
			LOG.error("write data error", e);
			// for exactly-once, we must throw exception.
			throw e;
		} finally {
			writeLock.unlock();
		}

	}

	/**
	 * need test validate
	 */
	protected boolean needValidateTest() {
		return false;
	}

	/**
	 * connection validate
	 */
	protected void validateTest() throws SQLException {

	}

	@Override
	public void close() throws IOException {
		if (scheduledExecutorService != null) {
			scheduledExecutorService.shutdown();
		}
	}

	public Integer getBatchMaxTimeout() {
		return batchMaxTimeout;
	}

	public void setBatchMaxTimeout(Integer batchMaxTimeout) {
		this.batchMaxTimeout = batchMaxTimeout;
	}

	public Integer getBatchMaxCount() {
		return batchMaxCount;
	}

	public void setBatchMaxCount(Integer batchMaxCount) {
		this.batchMaxCount = batchMaxCount;
	}
}
