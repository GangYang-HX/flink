package com.bilibili.bsql.clickhouse.shard.partitioner;

import com.bilibili.bsql.clickhouse.shard.executor.ClickHouseExecutor;
import com.bilibili.bsql.clickhouse.shard.failover.HashFailOver;
import com.bilibili.bsql.common.failover.BatchOutFormatFailOver;
import com.bilibili.bsql.common.failover.FailOverConfigInfo;
import com.google.common.collect.Multimap;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.bilibili.murmur3Hash.UDFMurmur3;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

public class HashPartitioner extends AbstractPartitioner {
	private static final Logger LOG = LoggerFactory.getLogger(HashPartitioner.class);
	private static final long serialVersionUID = 1L;
	private final List<RowData.FieldGetter> fieldGetters;
	private final String shardingFunc;
	public static final String MURMUR_HASH_3_32 = "murmurHash3_32";
	private final List<Integer> logicalTypes;
	private final List<List<ClickHouseExecutor>> shardExecutors;

	public HashPartitioner(final List<RowData.FieldGetter> fieldGetters, final List<Integer> logicalTypes,
	                       String shardingFunc, Multimap<Integer, ClickHouseConnection> shardConnections, int subtaskId) {

		super(subtaskId, shardConnections);
		this.fieldGetters = fieldGetters;
		this.logicalTypes = logicalTypes;
		this.shardingFunc = shardingFunc;
		this.shardExecutors = new ArrayList<>();
		LOG.info("use murmurhash? {}", shardingFunc.contains(MURMUR_HASH_3_32));
	}

	@Override
	public int select(final RowData record) {
		if (shardingFunc.contains(MURMUR_HASH_3_32)) {
			return (int) (murmurHashValue(record) % getShardNum());
		}
		return (javaHashValue(record) & 0x7fffffff) % getShardNum();
	}

	@Override
	public void addShardExecutors(int shardNum, ClickHouseExecutor clickHouseExecutor) {
		if (shardExecutors.size() == shardNum) {
			List<ClickHouseExecutor> executors = new ArrayList<>();
			shardExecutors.add(shardNum, executors);
		}
		shardExecutors.get(shardNum).add(clickHouseExecutor);
		LOG.info("add executor {} for shard_num {}", clickHouseExecutor, shardNum);
	}

	@Override
	public int getShardNum() {
		return shardConnections.keySet().size();
	}

	@Override
	public BatchOutFormatFailOver initFailOver(FailOverConfigInfo config) {
		HashFailOver hashFailOver = HashFailOver.createUselessHashFailOver();
		for (Integer shardNum : shardConnections.keySet()) {
			int replicaNum = shardConnections.get(shardNum).size();
			BatchOutFormatFailOver initBatchFailOver = new BatchOutFormatFailOver(config.getFailOverRate(),
					config.getFailOverTimeLength(), config.getBaseFailBackInterval(),
					config.getFailOverSwitchOn(), replicaNum, config.getMaxFailedIndexRatio());
			hashFailOver.addFailover(initBatchFailOver);
			LOG.info("subtask {} init {} hash failovers for shard: {}", subtaskId, replicaNum, shardNum);
		}
		return hashFailOver;
	}

	@Override
	public void write(List<RowData> records, int shardIndex, int replicaIndex) throws IOException {
		shardExecutors.get(shardIndex).get(replicaIndex).addBatch(records);
	}

	@Override
	public void flush(int shardIndex, int replicaIndex) throws IOException {
		shardExecutors.get(shardIndex).get(replicaIndex).executeBatch();
	}

	@Override
	public void close() throws SQLException {
        for (List<ClickHouseExecutor> shardExecutor : this.shardExecutors) {
        	for (ClickHouseExecutor executor: shardExecutor) {
		        executor.closeStatement();
	        }
        }
	}

	private Long murmurHashValue(RowData record) {
		UDFMurmur3 func = new UDFMurmur3();
		List<Object> args = new ArrayList<>();
		for (int i = 0; i< logicalTypes.size(); i++) {
			switch (logicalTypes.get(i)) {
				case Types.BIGINT:
					args.add((Long) fieldGetters.get(i).getFieldOrNull(record));
					break;
				case Types.INTEGER:
					args.add((Integer) fieldGetters.get(i).getFieldOrNull(record));
					break;
				case Types.SMALLINT:
					args.add((Short) fieldGetters.get(i).getFieldOrNull(record));
					break;
				case Types.VARCHAR:
				case Types.CHAR:
				case Types.LONGNVARCHAR:
					args.add(Objects.requireNonNull(fieldGetters.get(i).getFieldOrNull(record)).toString());
					break;
				case Types.TINYINT:
					args.add((Byte) fieldGetters.get(i).getFieldOrNull(record));
					break;
				default:
					//为了保护clickhouse集群不受单热点写入 这里直接让任务挂掉
					LOG.error("type type:{} do not match!", logicalTypes.get(i));
					throw new IllegalArgumentException("sharding key type do not match!");
			}
		}
		return (Long) func.evaluate(args.toArray());
	}
	private Integer javaHashValue(RowData record) {
		switch (logicalTypes.get(0)) {
			case Types.BIGINT:
			case Types.INTEGER:
			case Types.TINYINT:
				return Objects.hashCode( fieldGetters.get(0).getFieldOrNull(record));
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.LONGNVARCHAR:
				return Objects.hashCode(Objects.requireNonNull(fieldGetters.get(0).getFieldOrNull(record)).toString());
		}

		//为了保护clickhouse集群不受单热点写入 这里直接让任务挂掉
		LOG.error("type type:{} do not match!", logicalTypes.get(0));
		throw new IllegalArgumentException("sharding key type do not match!");
	}

}
