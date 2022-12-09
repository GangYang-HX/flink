package com.bilibili.bsql.hive.utils;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/4 6:40 下午
 */
public class HadoopPathUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopPathUtils.class);


	public static String verificationPath(String location) {
		try {
			HadoopFileSystem fileSystem = (HadoopFileSystem) FileSystem.get(new Path(location).toUri());
			location = fileSystem.getHadoopFileSystem().resolvePath(new org.apache.hadoop.fs.Path(location)).toString();
			LOG.info("Flink resolve path = {}", location);
		} catch (IOException e) {
			LOG.warn("Flink can not resolve path = {}", location, e);
			try {
				org.apache.flink.configuration.Configuration flinkConf = new org.apache.flink.configuration.Configuration();
				Configuration conf = org.apache.flink.runtime.util.HadoopUtils.getHadoopConfiguration(flinkConf);
				conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
				org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.fs.Path(location).toUri(), conf);
				location = fileSystem.resolvePath(new org.apache.hadoop.fs.Path(location)).toString();
				LOG.info("Hdfs resolve path = {}", location);
			} catch (Exception e1) {
				LOG.error("All Can not resolve path = {}", location, e1);
			}
		}
		return location;
	}
}
