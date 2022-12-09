/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common.cache.cachobj;

import org.apache.flink.table.data.RowData;

/**
 *
 * @author zhouxiaogang
 * @version $Id: SingleCacheObj.java, v 0.1 2020-10-25 18:04
zhouxiaogang Exp $$
 */
public class SingleCacheObj extends CacheObj<RowData> {

	public SingleCacheObj(RowData content) {
		super(content);
	}

	public CacheContentType getType() {
		return CacheContentType.SingleLine;
	}

	public RowData getContent() {
		return content;
	}

	public void setContent(RowData content) {
		this.content = content;
	}
}
