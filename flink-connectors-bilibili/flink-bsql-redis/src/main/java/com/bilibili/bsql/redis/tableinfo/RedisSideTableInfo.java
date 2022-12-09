/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis.tableinfo;

import java.io.Serializable;

import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.SideTableInfo;

import static com.bilibili.bsql.redis.tableinfo.RedisConfig.*;

import lombok.Data;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @author zhouxiaogang
 * @version $Id: RedisSideTableInfo.java, v 0.1 2020-10-13 18:47
 * zhouxiaogang Exp $$
 */
@Data
public class RedisSideTableInfo extends SideTableInfo implements Serializable {

	private String url;
	private String password;
	private Integer redisType;

	private boolean multiKey = false;

	private Integer keyIndex;
	private Integer valueIndex;
	private Integer retryMaxNum;
	private Long queryTimeOut;

	/**
	 * private int                 timeout;
	 * private String              maxTotal;
	 * private String              maxIdle;
	 * private String              minIdle;
	 * private String              database;
	 * private String              tableName;
	 * all useless above
	 */


	public RedisSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		this.url = helper.getOptions().get(BSQL_REDIS_URL);
		this.password = helper.getOptions().get(BSQL_REDIS_PSWD);
		this.redisType = helper.getOptions().get(BSQL_REDIS_TYPE);
		this.queryTimeOut = helper.getOptions().get(BSQL_QUERY_TIME_OUT);
		this.retryMaxNum = helper.getOptions().get(BSQL_RETRY_MAX_NUM);
		this.type = "redis";

		checkArgument(fields.size() == 2, "redis has only k/v for now");
		checkArgument(getPrimaryKeyIdx().size() == 1, "redis only support for one key");
		this.keyIndex = getPrimaryKeyIdx().get(0);
		this.valueIndex = 1 - this.keyIndex;
		checkArgument((multiKeyDelimitor != null) == (getPhysicalFields().get(valueIndex).getType().getTypeRoot() == ARRAY),
			"when defined delimitkey, value type must be array and vice versa ");

		if (multiKeyDelimitor != null) {
			checkArgument(getPhysicalFields().get(valueIndex).getType().getTypeRoot() == ARRAY);
			this.multiKey = true;
		}

		if (!"none".equals(getCacheType()) && multiKey) {
			throw new UnsupportedOperationException("redis multi query not support the cache for now!!!!! ");
		}
	}
}
