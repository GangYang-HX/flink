package com.bilibili.bsql.common.format;

import com.bilibili.bsql.common.FlinkBatchFlushException;
import com.bilibili.bsql.common.failover.BatchOutFormatFailOver;
import com.bilibili.bsql.common.failover.FailOverConfigInfo;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: zhuzhengjun
 * @date: 2021/2/23 3:18 下午
 */
public abstract class AbstractBatchOutputFormat extends CustomizeRichSinkFunction<RowData> implements CheckpointedFunction {

	private final static Logger LOG = LoggerFactory.getLogger(AbstractBatchOutputFormat.class);
	protected ScheduledExecutorService scheduledExecutorService;
	protected Integer batchMaxTimeout;
	protected Integer batchMaxCount;
	protected Integer maxRetries;
	protected int taskNumber;
	protected int clientCount;
	protected final Lock writeLock = new ReentrantLock();
	protected final List<List<RowData>> rowData = new ArrayList<>();
	protected  BatchOutFormatFailOver batchOutFormatFailOver;
	protected FailOverConfigInfo failOverConfigInfo;

    private final TypeInformation<RowData> typeInformation;
    private TypeSerializer<RowData> typeSerializer;
	/** Whether object reuse has been enabled or disabled. */
	private transient boolean isObjectReuseEnabled;

	public AbstractBatchOutputFormat(DataType dataType, DynamicTableSink.Context context){
        this.typeInformation = (TypeInformation<RowData>) context.createTypeInformation(dataType);
    }

	@Override
	public void doOpen(int taskNumber, int numTasks) throws IOException {
        this.typeSerializer = typeInformation.createSerializer(getRuntimeContext().getExecutionConfig());
		this.isObjectReuseEnabled = getRuntimeContext().getExecutionConfig().isObjectReuseEnabled();
		this.taskNumber = taskNumber;
		this.initArray();
		if (batchOutFormatFailOver == null) {
            batchOutFormatFailOver = new BatchOutFormatFailOver(failOverConfigInfo.getFailOverRate(),
                    failOverConfigInfo.getFailOverTimeLength(), failOverConfigInfo.getBaseFailBackInterval(),
                    failOverConfigInfo.getFailOverSwitchOn(), clientCount, failOverConfigInfo.getMaxFailedIndexRatio());
        }
		startFlushScheduler();
	}

	protected void startFlushScheduler() {
		if (null != scheduledExecutorService) {
			return;
		}
		if (batchMaxTimeout != null && batchMaxTimeout > 0) {
			scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
			Random random = new Random();
			scheduledExecutorService.scheduleWithFixedDelay(() -> {
				try {
					if (needValidateTest()) {
						validateTest();
					}
					writeLock.lock();
					flush();
				} catch (Exception e) {
					LOG.error("flush error,shutdown this scheduler", e);
					scheduledExecutorService.shutdown();
					throw new RuntimeException("scheduler flush error, will stopp task", e);
				} finally {
					writeLock.unlock();
				}
			}, batchMaxTimeout + random.nextInt(10_000), batchMaxTimeout, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public boolean doInvoke(RowData value, Context context) throws Exception {
		// attention : need single thread
		final int batchIndex = this.doGetIndex(value);
		try {
			writeLock.lock();
			boolean isFull = appendRowDataWithRecord(value, batchIndex);
			if (isFull) {
				execWithRetry(batchIndex);
			}
		} finally {
			writeLock.unlock();
		}
		return true;
	}


	private boolean appendRowDataWithRecord(RowData record, Integer index) {
		RowData value;
		if (isObjectReuseEnabled) {
			value = typeSerializer.copy(record);
		} else {
			value = record;
		}
		rowData.get(index).add(value);
		return rowData.get(index).size() >= batchMaxCount;
	}

	protected void clearRowDataAndRecord(Integer index) {
		this.rowData.get(index).clear();
	}


	@Override
	public void close() throws Exception {
		super.close();
	}


	/**
	 * for this class,init numPendingRequestsArray
	 */
	protected abstract void initArray() throws IOException;

	protected abstract void doWrite(List<RowData> records, int shardIndex, int replicaIndex) throws IOException;

	/**
	 * get shard index,if model was batch,index is always 0;
	 */
	protected abstract int doGetIndex(RowData record);


	private void exec(int batchIndex) throws Exception {
		int realSinkIndex = batchOutFormatFailOver.selectSinkIndex(batchIndex);
		try {
			doWrite(rowData.get(batchIndex), batchIndex, realSinkIndex);
			flushWithMetrics(batchIndex, realSinkIndex);
			clearRowDataAndRecord(batchIndex);
			batchOutFormatFailOver.failedIndexRecord(realSinkIndex, true);
		} catch (Exception e) {
			batchOutFormatFailOver.failedIndexRecord(realSinkIndex, false);
			throw e;
		}
	}


	/**
	 * implement this function if needed
	 */
	protected void execWithRetry(int batchIndex) throws FlinkBatchFlushException {
		for (int i = 1; i <= this.maxRetries; i++) {
			try {
				exec(batchIndex);
				break;
			} catch (Exception e) {
				sinkMetricsGroup.retryRecord();
				LOG.error(" executeBatch error, retry times = {}", i, e);
				if (i == this.maxRetries) {
					throw new FlinkBatchFlushException("unable to flush;retry times exceed!", e);
				}
			}
		}
	}

	private void flushWithMetrics(final int shardIndex, final int replicaIndex) throws Exception {
		long start = System.nanoTime();
		this.flush(shardIndex, replicaIndex);
		sinkMetricsGroup.rtFlushRecord(start);
	}

	/**
	 * flush
	 *
	 * @throws Exception Exception
	 */
	protected void flush() throws Exception {
		for (int batchIndex = 0; batchIndex < clientCount; ++batchIndex) {
			if (rowData.get(batchIndex).size() > 0) {
				execWithRetry(batchIndex);
			}
		}

	}


	/**
	 * flush by index
	 *
	 * @param shardIndex index
	 * @throws Exception Exception
	 */
	protected abstract void flush(int shardIndex, int replicaIndex) throws Exception;


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
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		//sync community
		//flush must set 0
		try {
			writeLock.lock();
			flush();
			LOG.info("sink checkpoint execute success.");
		} catch (Exception e) {
			LOG.error("sink flush failed when checkpoint!", e);
			throw e;
		} finally {
			writeLock.unlock();
		}

	}


	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		//do nothing
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

	public FailOverConfigInfo getFailOverConfigInfo() {
		return failOverConfigInfo;
	}

	public void setFailOverConfigInfo(FailOverConfigInfo failOverConfigInfo) {
		this.failOverConfigInfo = failOverConfigInfo;
	}
}
