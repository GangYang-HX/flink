package com.bilibili.bsql.hdfs.cache;

import com.bilibili.bsql.hdfs.tableinfo.HiveSideTableInfo;
import org.apache.flink.util.ShutdownHookUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: JinZhengyu
 * @Date: 2022/8/1 下午4:09
 */
public class HiveCacheFactory {
    private final static Logger LOG = LoggerFactory.getLogger(HiveCacheFactory.class);
    private volatile static Map<String, HiveTableCache> hiveTableCache;
    private final static HiveCacheFactory instance = new HiveCacheFactory();

    private HiveCacheFactory() {
        hiveTableCache = new HashMap<>(4);
        destroyTableCache();
    }

    public static HiveCacheFactory getInstance() {
        return instance;
    }

    public synchronized HiveTableCache getTableCache(HiveSideTableInfo tableInfo) throws Exception {
        String tableKey = tableKey(tableInfo);
        HiveTableCache tableCache = HiveCacheFactory.hiveTableCache.get(tableKey);
        if (tableCache == null) {
            HiveTableCache tmpTableCache = createTableCache(tableInfo);
            hiveTableCache.put(tableKey, tmpTableCache);
            return tmpTableCache;
        }
        return tableCache;
    }

    private String tableKey(HiveSideTableInfo tableInfo) {
        return tableInfo.getName();
    }

    private HiveTableCache createTableCache(HiveSideTableInfo tableInfo) throws Exception {
        return new HiveTableCache(tableInfo);
    }

    private void destroyTableCache() {
        Thread shutdownHook = new Thread(() -> hiveTableCache.forEach((tableKey, tableCache) -> {
            tableCache.shutdown();
        }));
        ShutdownHookUtil.addShutdownHookThread(shutdownHook, HiveCacheFactory.class.getSimpleName(), LOG);
    }
}
