/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.table.runtime.operators.join.latency.rockscache;

/**
 *
 * @author zhouxiaogang
 * @version $Id: CacheEntryType.java, v 0.1 2021-02-04 19:57
zhouxiaogang Exp $$
 */
public enum CacheEntryType {
	LEFT_INPUT,
	RIGHT_INPUT,
	OUTPUT;

	public static CacheEntryType valueOf(int ordinal) {
		if (ordinal < 0 || ordinal >= values().length) {
			throw new IndexOutOfBoundsException("Invalid ordinal");
		}
		return values()[ordinal];
	}
}
