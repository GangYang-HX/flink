/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common.cache.cachobj;

import java.util.List;

import org.apache.flink.table.data.RowData;

/**
 *
 * @author zhouxiaogang
 * @version $Id: MultipleCacheObj.java, v 0.1 2020-10-25 18:07
zhouxiaogang Exp $$
 */
public class MultipleCacheObj extends CacheObj<List<RowData>> {

	public MultipleCacheObj(List<RowData> content) {
		super(content);
	}

	public CacheContentType getType() {
		return CacheContentType.MultiLine;
	}

	public List<RowData> getContent() {
		return content;
	}

	public void setContent(List<RowData> content) {
		this.content = content;
	}
}
