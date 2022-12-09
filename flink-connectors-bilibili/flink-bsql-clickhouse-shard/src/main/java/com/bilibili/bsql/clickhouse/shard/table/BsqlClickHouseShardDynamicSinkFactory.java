package com.bilibili.bsql.clickhouse.shard.table;

import com.bilibili.bsql.clickhouse.shard.tableinfo.ClickHouseShardTableInfo;
import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

import static com.bilibili.bsql.clickhouse.shard.tableinfo.ClickHouseShardConfig.*;

/**
 * @author: zhuzhengjun
 * @date: 2021/2/19 5:26 下午
 */
public class BsqlClickHouseShardDynamicSinkFactory extends BsqlDynamicTableSinkFactory<ClickHouseShardTableInfo> {

	public static final String IDENTIFIER = "bsql-clickhouse-shard";


	@Override
	public ClickHouseShardTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {

		return new ClickHouseShardTableInfo(helper, context);
	}

	@Override
	public DynamicTableSink generateTableSink(ClickHouseShardTableInfo sinkTableInfo) {
		return new BsqlClickHouseShardDynamicSink(sinkTableInfo);
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_CLICKHOUSE_SHARD_URL);
		option.add(BSQL_CLICKHOUSE_SHARD_USERNAME);
		option.add(BSQL_CLICKHOUSE_SHARD_PASSWORD);
		option.add(BSQL_CLICKHOUSE_SHARD_DATABASE_NAME);
		option.add(BSQL_CLICKHOUSE_SHARD_TABLE_NAME);

	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_CLICKHOUSE_SHARD_MAX_RETRIES);
		option.add(BSQL_CLICKHOUSE_SHARD_PARTITION_STRATEGY);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

}
