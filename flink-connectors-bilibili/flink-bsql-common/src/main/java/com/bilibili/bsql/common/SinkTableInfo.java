/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common;

import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_BATCH_SIZE;
import static com.bilibili.bsql.common.keys.TableInfoKeys.BSQL_BATCH_TIMEOUT;
import static com.bilibili.bsql.common.failover.FailOverConfig.*;
import lombok.Data;

/**
 *
 * @author zhouxiaogang
 * @version $Id: SinkTableInfo.java, v 0.1 2020-10-13 14:05
zhouxiaogang Exp $$
 */
@Data
public abstract class SinkTableInfo extends TableInfo {

	public Integer batchMaxTimeout;
	public Integer batchMaxCount;
	/**
	 * failOverRate
	 */
	private Double failOverRate;
	/**
	 * statistics time interval
	 */
	private Integer failOverTimeLength;
	/**
	 * failOver time out value
	 */
	private Integer baseFailBackInterval;
	/**
	 * failOver  Switch
	 */
	private Boolean failOverSwitchOn;
	/**
	 * failOver index max ratios
	 *  calc : Sum(failedIndex)/Sum(allIndex)
	 */
	private Double maxFailedIndexRatio;

	public SinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);

		this.batchMaxCount = helper.getOptions().get(BSQL_BATCH_SIZE);
		this.batchMaxTimeout = helper.getOptions().get(BSQL_BATCH_TIMEOUT);
		this.failOverRate = helper.getOptions().get(BSQL_FAILOVER_RATE);
		this.failOverTimeLength = helper.getOptions().get(BSQL_FAILOVER_TIME_LENGTH);
		this.baseFailBackInterval = helper.getOptions().get(BSQL_BASE_FAIL_BACK_INTERVAL);
		this.failOverSwitchOn = helper.getOptions().get(BSQL_FAILOVER_SWITCH_ON);
		this.maxFailedIndexRatio = helper.getOptions().get(BSQL_MAX_FAILED_INDEX_RATIO);
	}
}
