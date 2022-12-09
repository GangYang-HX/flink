/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bili.writer.spliter;

import org.apache.flink.core.fs.Path;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.bili.writer.partition.PartitionPathUtils.*;

/**
 *
 * @author zhouxiaogang
 * @version $Id: DefaultPathSpliter.java, v 0.1 2020-06-29 19:55
zhouxiaogang Exp $$
 */
public class DefaultPathSplitter implements PathSplitter {
	public List<String> splitPath(boolean pathContainPartitionKey, List<String> partitionKeys, Path path) {
		if (pathContainPartitionKey) {
			return new ArrayList<>(extractPartitionSpecFromPath(path).values());
		} else {
			return new ArrayList<>(extractPartitionSpecFromPathWithoutPartitionKey(partitionKeys, path).values());
		}
	}
}
