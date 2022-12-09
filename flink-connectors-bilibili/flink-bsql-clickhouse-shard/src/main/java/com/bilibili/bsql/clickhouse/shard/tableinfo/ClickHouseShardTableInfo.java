package com.bilibili.bsql.clickhouse.shard.tableinfo;

import com.bilibili.bsql.common.SinkTableInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.Arrays;

import static com.bilibili.bsql.clickhouse.shard.tableinfo.ClickHouseShardConfig.*;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @author: zhuzhengjun
 * @date: 2021/2/19 10:58 上午
 */
@Data
public class ClickHouseShardTableInfo extends SinkTableInfo implements Serializable {
	private static final String CUR_TYPE = "bsql-clickhouse-shard";

	private String url;
	private String username;
	private String password;

	private Integer maxRetries;
	private Boolean writeLocal;
	private String partitionStrategy;
	private String partitionKey;
	private String tableName;
	private String database;
	private String charset;
	private Long socketTimeout;
	private Long connectionTimeout;



	public ClickHouseShardTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		this.url = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_URL);
		this.username = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_USERNAME);
		this.password = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_PASSWORD);
		this.maxRetries = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_MAX_RETRIES);
		this.partitionStrategy = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_PARTITION_STRATEGY);
		this.database = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_DATABASE_NAME);
		this.tableName = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_TABLE_NAME);
		this.charset = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_CHARSET);
		this.socketTimeout = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_SOCKET_TIMEOUT);
		this.connectionTimeout = helper.getOptions().get(BSQL_CLICKHOUSE_SHARD_CONNECTION_TIMEOUT);
		checkArgument(StringUtils.isNotEmpty(url), String.format("sink表：%s 没有设置url属性", getName()));
		checkArgument(StringUtils.isNotEmpty(username), String.format("sink表：%s 没有设置username属性", getName()));
		checkArgument(StringUtils.isNotEmpty(password), String.format("sink表：%s 没有设置password属性", getName()));
		checkArgument(StringUtils.isNotEmpty(database), String.format("sink表：%s 没有设置databaseName属性", getName()));
//		checkArgument(Arrays.asList("hash", "balanced", "shuffle").contains(partitionStrategy), String.format("sink表：%s 未配置shard分区策略", getName()));
	}

}
