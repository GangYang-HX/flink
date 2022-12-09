package com.bilibili.bsql.hive.table.splitter;

import org.apache.flink.bili.writer.spliter.DefaultPathSplitter;
import org.apache.flink.core.fs.Path;

import java.util.List;


public class BsqlPathSplitter extends DefaultPathSplitter {


	/**
	 * only support for path like /~~=~~/
	 * @param path path
	 * @return list
	 */
    @Override
	public List<String> splitPath(boolean pathContainPartitionKey, List<String> partitionKeys, Path path) {
		return super.splitPath(pathContainPartitionKey, partitionKeys, path);
    }
}
