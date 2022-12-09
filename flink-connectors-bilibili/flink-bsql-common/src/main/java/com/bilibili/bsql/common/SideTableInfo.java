/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common;

import com.bilibili.bsql.common.cache.AbsSideCache;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.common.keys.TableInfoKeys.*;

import lombok.Data;

/**
 * @author zhouxiaogang
 * @version $Id: SideTableInfo.java, v 0.1 2020-10-13 14:15
 * zhouxiaogang Exp $$
 */
@Data
public abstract class SideTableInfo extends TableInfo {

	public int cacheSize = 10000;
	public long cacheTimeout = 60 * 1000;
	public String cacheType = "none";

	public Integer asyncCapacity = 100;
	public Integer asyncTimeout = 10000;
	protected AbsSideCache sideCache;
	public String multiKeyDelimitor;

	public SideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);

		this.cacheSize = helper.getOptions().get(BSQL_CACHE_SIZE);
		this.cacheTimeout = helper.getOptions().get(BSQL_CACHE_TTL);
		this.cacheType = helper.getOptions().get(BSQL_CACHE);

		this.multiKeyDelimitor = helper.getOptions().get(BSQL_MULTIKEY_DELIMITKEY);
	}
}
