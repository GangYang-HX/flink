/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common;

import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import lombok.Data;

/**
 *
 * @author zhouxiaogang
 * @version $Id: SourceTableInfo.java, v 0.1 2020-10-13 13:55
zhouxiaogang Exp $$
 */
@Data
public abstract class SourceTableInfo extends TableInfo {
	/**
	 * this class doesn't have much to do
	 *
	 * timezone, source suffix is no longer needed
	 *
	 *
	 * eventTimeField, processTimeField no longer needed
	 *
	 * https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/create.html#create-table
	 *
	 * */

	public SourceTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
	}
}
