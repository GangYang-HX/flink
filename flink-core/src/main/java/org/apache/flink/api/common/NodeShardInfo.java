/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package org.apache.flink.api.common;

import java.io.Serializable;

/**
 *
 * @author zhouxiaogang
 * @version $Id: NodeShardInfo.java, v 0.1 2021-02-07 20:52
zhouxiaogang Exp $$
 */
public class NodeShardInfo implements Serializable {
	public static final int UNKNOWN_SHARD_NUM = -1;

	public final boolean needToKeepPrevShard;
	public final int shardNumber;

	public NodeShardInfo(boolean needToKeepPrevShard, int shardNumber) {
		this.needToKeepPrevShard = needToKeepPrevShard;
		this.shardNumber = shardNumber;
	}
}
