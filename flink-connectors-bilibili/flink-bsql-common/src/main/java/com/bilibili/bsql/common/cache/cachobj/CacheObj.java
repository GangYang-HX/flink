/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.bilibili.bsql.common.cache.cachobj;

import java.util.List;

import org.apache.flink.table.data.RowData;

/**
 * Reason:
 * Date: 2018/9/10
 */
public abstract class CacheObj<T> {

	public T content;

	public CacheObj(T content) {
		this.content = content;
	}

	public static CacheObj buildCacheObj(CacheContentType type, Object content) {
		if (type == CacheContentType.MissVal) {
			return CacheMissVal.getMissKeyObj();
		} else if (type == CacheContentType.SingleLine) {
			return new SingleCacheObj((RowData) content);
		} else if (type == CacheContentType.MultiLine) {
			return new MultipleCacheObj((List<RowData>) content);
		} else {
			throw new UnsupportedOperationException("only three content type exist");
		}
	}

	public abstract CacheContentType getType();

	public abstract T getContent();

	public abstract void setContent(T content);
}
