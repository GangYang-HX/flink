package com.bilibili.bsql.hbase.function;

import com.bilibili.bsql.common.function.AsyncSideFunction;
import com.bilibili.bsql.common.utils.BiliThreadFactory;
import com.bilibili.bsql.hbase.tableinfo.HbaseSideTableInfo;
import com.bilibili.bsql.hbase.util.HBaseSerde;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;


/**
 * @author zhuzhengjun
 * @date 2020/11/12 4:51 下午
 */
public class HbaseLookupFunction extends AsyncSideFunction<HbaseSideTableInfo> {


	private static final Logger LOG = LoggerFactory.getLogger(HbaseLookupFunction.class);
	private static final int DEFAULT_POOL_SIZE = 20;
	private final String hTableName;
	private final String nullStringLiteral;

	private transient Connection hConnection;
	private transient HTable table;
	private final String zkQuorum;
	private final String zkParent;
	public ThreadPoolExecutor callbackThreadPool;
	private ThreadLocal<HBaseSerde> serde;

	public HbaseLookupFunction(HbaseSideTableInfo sideTableInfo, LookupTableSource.LookupContext context) {
		super(sideTableInfo, context);
		this.sideTableInfo = sideTableInfo;
		this.hTableName = sideTableInfo.getTableName();
		this.nullStringLiteral = sideTableInfo.getNullStringLiteral();
		this.zkParent = sideTableInfo.getParent();
		this.zkQuorum = sideTableInfo.getHost();
	}


	@Override
	protected void open0(FunctionContext context) throws Exception {
		LOG.info("start open ...");
//        Configuration config = prepareRuntimeConfiguration();
		//flink 1.11 未提供 异步join实现
		Configuration config = getHbaseClient(zkQuorum, zkParent);
		try {
			hConnection = ConnectionFactory.createConnection(config);
			table = (HTable) hConnection.getTable(TableName.valueOf(hTableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("Table '{}' not found ", hTableName, tnfe);
			throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to HBase.", ioe);
			throw new RuntimeException("Cannot create connection to HBase.", ioe);
		}
		callbackThreadPool = new ThreadPoolExecutor(DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE,
			60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<>(), new BiliThreadFactory("hbase-aysnc1"));
		hbaseRegionLocatorPreload();
		//todo 校验
		this.serde = ThreadLocal.withInitial(() -> new HBaseSerde(this.sideTableInfo.getHBaseTableSchema(), nullStringLiteral, null, null, null, null));
		LOG.info("end open.");

	}

	public void hbaseRegionLocatorPreload() {
		for (int i = 0; i < 5; i++) {
			try {

				List<HRegionLocation> HRegionA =
					hConnection.getRegionLocator(TableName.valueOf(hTableName)).getAllRegionLocations();
				for (HRegionLocation location : HRegionA) {
					LOG.info("locator of cluster: {}", location);
				}

			} catch (Exception e) {
				//ignore
			}
		}
	}

	private Configuration getHbaseClient(String zkQuorum, String zkParent) {
		Configuration config = HBaseConfiguration.create();
		config.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "5000");
		config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "15000");
		config.set(HConstants.HBASE_CLIENT_PAUSE, "5");
		config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "3");
		config.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
		config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkParent);
		config.set(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, "3");

		return config;
	}


	@Override
	protected void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
		sideMetricsWrapper.tps();
		long start = System.nanoTime();
		Object rowKey = inputs[0];
		Get get = serde.get().createGet(rowKey);
		if (get != null) {
			callbackThreadPool.submit(() -> {
				Result result = null;
				try {
					result = table.get(get);
				} catch (IOException ioException) {
					sideMetricsWrapper.rpcCallbackFailure();
				}
				if (result != null && !result.isEmpty()) {
					// parse and collect
					RowData rowData = serde.get().convertToRow(result);
					resultFuture.complete(Collections.singletonList(rowData));
					sideMetricsWrapper.sideJoinSuccess();
					sideMetricsWrapper.rtQuerySide(start);

				} else {
					resultFuture.complete(null);
				}


			});

		} else {
			LOG.warn("Hbase get is null for rowKey:{}", rowKey);
			resultFuture.complete(null);
		}
	}

	@Override
	protected void close0() throws Exception {
		LOG.info("start close ...");
		if (null != table) {
			try {
				table.close();
				table = null;
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close table", e);
			}
		}
		if (null != hConnection) {
			try {
				hConnection.close();
				hConnection = null;
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close connection", e);
			}
		}
		if (null != callbackThreadPool) {
			callbackThreadPool.shutdown();
		}
		LOG.info("end close.");

	}

	@Override
	protected Object createCacheKey(Object... inputs) {
		return null;
	}
}
