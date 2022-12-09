package com.bilibili.bsql.hdfs.cache;

import com.bilibili.bsql.hdfs.tableinfo.HdfsSideTableInfo;
import org.apache.flink.util.ShutdownHookUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HdfsCacheFactory.java
 * @description This is the description of HdfsCacheFactory.java
 * @createTime 2020-10-22 21:20:00
 */
public class HdfsCacheFactory {
	private final static Logger LOG = LoggerFactory.getLogger(HdfsCacheFactory.class);

	private volatile static Map<String, HdfsTableCache> hdfsTableCache;
	private final static HdfsCacheFactory instance = new HdfsCacheFactory();

	private HdfsCacheFactory() {
		hdfsTableCache = new HashMap<>(4);
		destroyTableCache();
	}

	public static HdfsCacheFactory getInstance() {
		return instance;
	}

	public synchronized HdfsTableCache getTableCache(HdfsSideTableInfo tableInfo) throws Exception {
		String tableKey = tableKey(tableInfo);
		HdfsTableCache tableCache = hdfsTableCache.get(tableKey);
		if (tableCache == null) {
			HdfsTableCache tmpTableCache = createTableCache(tableInfo);
			hdfsTableCache.put(tableKey, tmpTableCache);
			return tmpTableCache;
		}
		return tableCache;
	}

	private String tableKey(HdfsSideTableInfo tableInfo) {
		return tableInfo.getName();
	}

	private HdfsTableCache createTableCache(HdfsSideTableInfo tableInfo) throws Exception {
		return new HdfsTableCache(tableInfo);
	}

	private void destroyTableCache() {
		Thread shutdownHook = new Thread(() -> hdfsTableCache.forEach((tableKey, tableCache) -> {
			tableCache.shutdown();
		}));
		ShutdownHookUtil.addShutdownHookThread(shutdownHook, HdfsCacheFactory.class.getSimpleName(), LOG);
	}
}
