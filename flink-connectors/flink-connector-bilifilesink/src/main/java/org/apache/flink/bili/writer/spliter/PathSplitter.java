/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bili.writer.spliter;

import java.util.List;

import org.apache.flink.core.fs.Path;

/**
 *
 * @author zhouxiaogang
 * @version $Id: PathSpliter.java, v 0.1 2020-06-29 19:45
zhouxiaogang Exp $$
 */
public interface PathSplitter {
	String DEFAULT = "default";
	String CUSTOM = "custom";

	List<String> splitPath(boolean pathContainPartitionKey, List<String> partitionKeys, Path path);

	static PathSplitter create(
		ClassLoader userClassLoader,
		String splitterKind,
		String splitterClass) {
		switch (splitterKind) {
			case DEFAULT:
				return new DefaultPathSplitter();
			case CUSTOM:
				try {
					return (PathSplitter) userClassLoader.loadClass(splitterClass).newInstance();
				} catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
					throw new RuntimeException(
						"Can not new instance for custom class from " + splitterClass, e);
				}
			default:
				throw new UnsupportedOperationException(
					"Unsupported extractor kind: " + splitterKind);
		}
	}
}
