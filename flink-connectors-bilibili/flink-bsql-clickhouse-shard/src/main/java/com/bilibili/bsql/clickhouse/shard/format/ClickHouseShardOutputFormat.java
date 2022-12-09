package com.bilibili.bsql.clickhouse.shard.format;

import com.bilibili.bsql.clickhouse.shard.connection.ClickHouseConnectionProvider;
import com.bilibili.bsql.clickhouse.shard.converter.ClickHouseRowConverter;
import com.bilibili.bsql.clickhouse.shard.executor.ClickHouseBatchExecutor;
import com.bilibili.bsql.clickhouse.shard.executor.ClickHouseExecutor;
import com.bilibili.bsql.clickhouse.shard.partitioner.BalancedPartitioner;
import com.bilibili.bsql.clickhouse.shard.partitioner.ClickHousePartitioner;
import com.bilibili.bsql.clickhouse.shard.partitioner.HashPartitioner;
import com.bilibili.bsql.clickhouse.shard.partitioner.ShufflePartitioner;
import com.bilibili.bsql.clickhouse.shard.tableinfo.ClickHouseShardTableInfo;
import com.bilibili.bsql.common.format.AbstractBatchOutputFormat;
import com.google.common.collect.Multimap;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: zhuzhengjun
 * @date: 2021/2/23 3:56 下午
 */
public class ClickHouseShardOutputFormat extends AbstractBatchOutputFormat {
	private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardOutputFormat.class);
	private final ClickHouseShardTableInfo clickHouseShardTableInfo;
	private ClickHousePartitioner clickHousePartitioner;
	private transient ClickHouseConnection connection;
	private static final Pattern PATTERN = Pattern.compile("Distributed\\((.*?), (.*?), (.*?)(, (.*))?\\)");

	private String remoteTable;
	private transient Multimap<Integer, ClickHouseConnection> shardConnections;
	//	private transient int[] batchCounts;
	private final List<String> fields;
	private transient boolean closed;
	protected final int[] sqlTypes;
	private final ClickHouseRowConverter clickHouseRowConverter;
	private final List<LogicalType> fieldDataTypes;
	private List<String> shardingKeys;
	private final List<RowData.FieldGetter> fieldGetters;
	private final List<Integer> shardingKeysDataTypes;


	public ClickHouseShardOutputFormat(final ClickHouseShardTableInfo clickHouseShardTableInfo, DataType dataType, DynamicTableSink.Context context, final List<String> fields, final int[] sqlTypes, final List<LogicalType> fieldDataTypes) {
		super(dataType,context);
	    this.clickHouseShardTableInfo = clickHouseShardTableInfo;
		this.fieldGetters = new ArrayList<>();
		this.shardingKeysDataTypes = new ArrayList<>();
		this.fields = fields;
		this.closed = false;
		this.sqlTypes = sqlTypes;
		this.clickHouseRowConverter = new ClickHouseRowConverter(sqlTypes, context.createDataStructureConverter(dataType));
		this.batchMaxCount = clickHouseShardTableInfo.getBatchMaxCount();
		this.batchMaxTimeout = clickHouseShardTableInfo.getBatchMaxTimeout();
		this.maxRetries = clickHouseShardTableInfo.getMaxRetries();
		this.fieldDataTypes = fieldDataTypes;

	}

	private ClickHousePartitioner createClickHouseConnectionProvider() throws Exception {
		final String partitionStrategy = this.clickHouseShardTableInfo.getPartitionStrategy();
		ClickHousePartitioner partitioner;
		switch (partitionStrategy) {
			case "shuffle": {
				partitioner = new ShufflePartitioner(shardConnections, clickHouseShardTableInfo.getBatchMaxCount(),
                        getRuntimeContext().getIndexOfThisSubtask());
				break;
			}
			case "hash": {
                //为保证hash写入语义，关闭failOver开关
                //failOverConfigInfo.setFailOverSwitchOn(false);
                this.shardingKeys = getShardingKeys();
                initFieldGetter();
				partitioner = new HashPartitioner(this.fieldGetters, this.shardingKeysDataTypes,
                        ClickHouseConnectionProvider.shardingFunc, shardConnections, getRuntimeContext().getIndexOfThisSubtask());
                batchOutFormatFailOver = partitioner.initFailOver(failOverConfigInfo);
				break;
			}
			default: {
				partitioner = new BalancedPartitioner(shardConnections, clickHouseShardTableInfo.getBatchMaxCount(),
                        getRuntimeContext().getIndexOfThisSubtask());
				break;
			}
		}
		LOG.info("use partitioner: {}", partitioner.getClass().getSimpleName());
		return partitioner;
	}

	private List<String> getShardingKeys() throws IOException {
		try {
			final String engine_full = ClickHouseConnectionProvider.queryTableEngine(clickHouseShardTableInfo);
			Optional<List<String>> shardingKeys = ClickHouseConnectionProvider.getShardingKeys(engine_full);
			if (!shardingKeys.isPresent()) {
				throw new IOException("can not find sharding keys by engine_full in system.tables.");
			}
			return shardingKeys.get();
		} catch (SQLException throwable) {
			throw new IOException("do get sharding keys failed.", throwable);
		}
	}

	private void initFieldGetter() throws IllegalArgumentException {
		for (String key : this.shardingKeys) {
			final int index = this.fields.indexOf(key);
			if (index == -1) {
				throw new IllegalArgumentException("Partition key `" + key + "` not found in table schema");
			}
			final RowData.FieldGetter getter = RowData.createFieldGetter(this.fieldDataTypes.get(index), index);
			shardingKeysDataTypes.add(this.sqlTypes[index]);
			fieldGetters.add(getter);
		}
		if (fieldGetters.isEmpty()) {
			throw new IllegalArgumentException("ddl has no parameters for setting partition key!");
		}
	}

	@Override
	protected void initArray() throws IOException {
		try {
			this.connection = ClickHouseConnectionProvider.getConnection(clickHouseShardTableInfo);
			this.establishShardConnections();
            this.clickHousePartitioner = createClickHouseConnectionProvider();
            this.initializeExecutors();
		} catch (Exception e) {
			throw new IOException("unable to establish connection to ClickHouse", e);
		}
        //init rowData num
        clientCount = clickHousePartitioner.getShardNum();
        for (int i = 0; i < clientCount; ++i) {
            List<RowData> row = new ArrayList<>();
            rowData.add(row);
        }
	}

	private void establishShardConnections() throws IOException {
		try {
			final String engine = ClickHouseConnectionProvider.queryTableEngine(clickHouseShardTableInfo);
			final Matcher matcher = PATTERN.matcher(engine);

			if (!matcher.find()) {
				throw new IOException("table `" + this.clickHouseShardTableInfo.getDatabase() + "`.`" + this.clickHouseShardTableInfo.getTableName() + "` is not a Distributed table");
			}
			final String remoteCluster = removeQuota(matcher.group(1));
			final String remoteDatabase = removeQuota(matcher.group(2));
			this.remoteTable = removeQuota(matcher.group(3));
			this.shardConnections = ClickHouseConnectionProvider.getShardConnections(clickHouseShardTableInfo, remoteCluster, remoteDatabase);

		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private void initializeExecutors() throws SQLException {
		String sql = buildInsertSql(this.remoteTable, fields);
		int index = -1;
		int tempShardNum = -1;
        for (Map.Entry<Integer, ClickHouseConnection> entry : this.shardConnections.entries()) {
            int shardNum = entry.getKey();
            ClickHouseConnection con = entry.getValue();
            ClickHouseExecutor executor = new ClickHouseBatchExecutor(sql, this.clickHouseRowConverter, clickHouseShardTableInfo.getBatchMaxCount(), clickHouseShardTableInfo.getMaxRetries(), sqlTypes, sinkMetricsGroup);
            executor.prepareStatement(con);
            //group by shard_num
            if (tempShardNum != shardNum) {
                this.clickHousePartitioner.addShardExecutors(++index, executor);
            } else {
                this.clickHousePartitioner.addShardExecutors(index, executor);
            }
            tempShardNum = shardNum;
        }
	}

    @Override
    protected void flush() throws Exception {
        if (clickHousePartitioner instanceof HashPartitioner) {
            //hash can not flush randomly
            super.flush();
        } else {
            List<RowData> datas = new ArrayList<>();
            for (int batchIndex = 0; batchIndex < clientCount; ++batchIndex) {
                if (rowData.get(batchIndex).size() > 0) {
                    datas.addAll(rowData.get(batchIndex));
                    clearRowDataAndRecord(batchIndex);
                }
            }
            clickHousePartitioner.batchFlush(datas);
        }
    }

    private String removeQuota(String str) {
		if (str.contains("'")) return str.replace("'", "");
		else return str;
	}

	private String buildInsertSql(String tableName, List<String> fields) {
		String sqlTmp = "INSERT INTO " + tableName + " (${fields}) values (${placeholder})";
		StringBuilder fieldsStr = new StringBuilder();
		StringBuilder placeholder = new StringBuilder();

		for (String fieldName : fields) {
			fieldsStr.append(",`").append(fieldName).append("`");
			placeholder.append(",?");
		}

		fieldsStr = new StringBuilder(fieldsStr.toString().replaceFirst(",", ""));
		placeholder = new StringBuilder(placeholder.toString().replaceFirst(",", ""));
		sqlTmp = sqlTmp.replace("${fields}", fieldsStr.toString()).replace("${placeholder}", placeholder.toString());
		return sqlTmp;
	}

	@Override
	protected void doWrite(List<RowData> records, int shardIndex, int replicaIndex) throws IOException {
	    this.clickHousePartitioner.write(records, shardIndex, replicaIndex);
    }

	@Override
	protected int doGetIndex(final RowData record) {
		return this.clickHousePartitioner.select(record);
	}

    @Override
    protected void flush(int shardIndex, int replicaIndex) throws Exception {
        this.clickHousePartitioner.flush(shardIndex, replicaIndex);
    }


	@Override
	public void close() throws Exception {
		if (!this.closed) {
			this.closed = true;
			try {
				this.flush();
			} catch (Exception e) {
				LOG.warn("Writing records to ClickHouse failed.", e);
				throw new Exception(e);

			}
			this.closeConnection();
		}

	}

	private void closeConnection() {
		if (this.connection != null) {
			try {
				this.clickHousePartitioner.close();
                ClickHouseConnectionProvider.closeConnections();

			} catch (SQLException se) {
				LOG.warn("ClickHouse connection could not be closed: {}", se.getMessage());
			} finally {
				this.connection = null;
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		super.snapshotState(context);
	}
}
