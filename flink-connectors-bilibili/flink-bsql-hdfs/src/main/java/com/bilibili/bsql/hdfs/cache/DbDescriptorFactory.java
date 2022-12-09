package com.bilibili.bsql.hdfs.cache;

import com.bilibili.bsql.hdfs.cache.memory.HiveSideMemoryDbDescriptor;
import com.bilibili.bsql.hdfs.cache.memory.MemoryDbDescriptor;
import com.bilibili.bsql.hdfs.cache.rocksdb.HiveSideRocksDbDescriptor;
import com.bilibili.bsql.hdfs.cache.rocksdb.RocksDbDescriptor;
import org.apache.flink.table.expressions.UUID;

import java.io.File;

public class DbDescriptorFactory {

    public static DbDescriptor getDbDescriptor(String basePath, String taskUuid, String localPath, long memoryThresholdSize, String delimiter, int keyIndex) {
        File localFile = new File(localPath);
        long localFileSize = getTotalSizeOfFilesInDir(localFile);
        DbDescriptor db;
        if (localFileSize <= memoryThresholdSize) {
            db = getMemoryDbDescriptor(basePath + "/" + taskUuid, memoryThresholdSize, delimiter, keyIndex);

        } else {
            db = getRocksDbDescriptor(basePath, taskUuid, delimiter, keyIndex);
        }
        return db;
    }


    public static DbDescriptor getMemoryDbDescriptor(String basePath, long memoryThresholdSize, String delimiter, int keyIndex) {
        return MemoryDbDescriptor
                .MemoryDbDescriptorBuilder
                .newBuilder()
                .withMemorySize(memoryThresholdSize)
                .withDelimiter(delimiter)
                .whitKeyIndex(keyIndex)
                .withRootPath(basePath)
                .build();
    }

    public static DbDescriptor getRocksDbDescriptor(String basePath, String taskUuid, String delimiter, int keyIndex) {
        return RocksDbDescriptor.RocksDbDescriptorBuilder.newBuilder()
                .withRootPath(basePath)
                .withDelimiter(delimiter)
                .whitKeyIndex(keyIndex)
                .withTaskUuid(taskUuid)
                .build();
    }


    public static DbDescriptor getHiveSideRocksDbDescriptor(String basePath, String taskUuid, String delimiter, int keyIndex) {
        return HiveSideRocksDbDescriptor.HiveSideRocksDbDescriptorBuilder.newBuilder()
                .withRootPath(basePath)
                .withDelimiter(delimiter)
                .withKeyIndex(keyIndex)
                .withTaskUuid(taskUuid)
                .build();
    }

    public static DbDescriptor getHiveSideMemoryDbDescriptor(String basePath, String taskUuid, String delimiter, int keyIndex) {
        return HiveSideMemoryDbDescriptor.HiveSideMemoryDbDescriptorBuilder.newBuilder()
                .withRootPath(basePath + "/" + taskUuid)
                .withDelimiter(delimiter)
                .withKeyIndex(keyIndex)
                .build();
    }


    private static long getTotalSizeOfFilesInDir(final File file) {
        if (file.isFile()) {
            return file.length();
        }
        final File[] children = file.listFiles();
        long total = 0;
        if (children != null) {
            for (final File child : children) {
                total += getTotalSizeOfFilesInDir(child);
            }
        }
        return total;
    }
}
