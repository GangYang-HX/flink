package com.bilibili.bsql.hdfs.cache;

import com.alibaba.fastjson.JSONArray;
import com.bilibili.bsql.common.utils.FileUtil;
import com.bilibili.bsql.common.utils.ObjectUtil;
import com.bilibili.bsql.hdfs.tableinfo.HiveSideTableInfo;
import com.bilibili.bsql.hdfs.util.HiveMetaWrapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: JinZhengyu
 * @Date: 2022/8/1 下午4:07
 */
public class HiveTableCache {
    private final static Logger LOG = LoggerFactory.getLogger(HiveTableCache.class);
    private final HiveSideTableInfo tableInfo;
    private String DB_RESOURCE_ROOT;
    private static String localDirs;
    private volatile DbDescriptor curDb;
    private volatile String curHdfsPath;
    private final static int HOUR_TABLE_DETECTOR_PERIOD = 5;
    private final static int DAY_TABLE_DETECTOR_PERIOD = 10;
    private ScheduledExecutorService scheduledExecutorService;
    private Integer keyIndex;
    private final static String FLINK_HIVE_SIDE = "flink-hive-side";
    private AtomicBoolean isClose;

    public HiveTableCache(HiveSideTableInfo tableInfo) throws Exception {
        this.tableInfo = tableInfo;
        String tmpDir = System.getenv("LOCAL_DIRS");
        LOG.info("tmpDir tmp path: {}", tmpDir);
        if (tmpDir == null) {
            localDirs = System.getProperty("java.io.tmpdir");
        } else {
            String[] localDirArray = tmpDir.split(",");
            Random random = new Random();
            localDirs = localDirArray[random.nextInt(localDirArray.length)];
        }
        LOG.info("localDirs tmp path: {}", localDirs);
        String joinKey = tableInfo.getJoinKey();
        String[] allFields = tableInfo.getFieldNames();
        for (int i = 0; i < allFields.length; i++) {
            if (StringUtils.equalsIgnoreCase(joinKey, allFields[i])) {
                keyIndex = i;
                break;
            }
        }
        if (keyIndex == null) {
            throw new Exception(String.format("The table fields does not contain the joinKey field:%s, please check.", joinKey));
        }
        init();
    }

    private synchronized void init() {
        validPartition(tableInfo);
        initResourceRoot();
        curDb = fullCacheLoadInit();
        isClose = new AtomicBoolean(false);
        startDetector();
    }


    private void initResourceRoot() {
        LOG.info("java.io.tmpdir: {}", localDirs);
        if (StringUtils.isNotBlank(localDirs)) {
            this.DB_RESOURCE_ROOT = localDirs + "/" + FLINK_HIVE_SIDE + "-" + UUID.randomUUID() + "/";
            LOG.info("resource root tmp path: {}", DB_RESOURCE_ROOT);
        } else {
            throw new RuntimeException("the environment variable java.io.tmpdir directory is empty.");
        }
    }


    private DbDescriptor createTmpDB(String remotePath) throws Exception {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.newInstance(FileSystem.getDefaultUri(conf), conf, this.tableInfo.getHdfsUser());
            HiveMetaWrapper.validPath(fs, remotePath, tableInfo.isCheckSuccessFile());
            String pidString = ManagementFactory.getRuntimeMXBean().getName();
            String pid = pidString.split("@")[0];
            String dbParent = DB_RESOURCE_ROOT + remotePath.replaceAll("/|=", "_") + "_" + pid;
            boolean ok = new File(dbParent).mkdirs();
            LOG.info("create db_resource :{} is ok:{}", dbParent, ok);
            String uuid = UUID.randomUUID().toString().replace("-", "");
            String dbLocalHdfsFilePath = dbParent + "/" + uuid + "/hdfs";
            LOG.info("Local hdfs file path = {}", dbLocalHdfsFilePath);
            boolean isSuccess = downloadFileFromHdfs(fs, remotePath, dbLocalHdfsFilePath);
            if (!isSuccess) {
                throw new RuntimeException("download file:" + remotePath + " error");
            }
            HiveMetaWrapper.validPath(fs, remotePath, tableInfo.isCheckSuccessFile());
            DbDescriptor dbDescriptor;
            if (tableInfo.isUseMem()) {
                dbDescriptor = DbDescriptorFactory.getHiveSideMemoryDbDescriptor(dbParent, uuid, tableInfo.getDelimiter(), keyIndex);
            } else {
                dbDescriptor = DbDescriptorFactory.getHiveSideRocksDbDescriptor(dbParent, uuid, tableInfo.getDelimiter(), keyIndex);
            }

            boolean loadSuccess = dbDescriptor.loadData2Db(dbLocalHdfsFilePath, tableInfo);
            if (!loadSuccess) {
                throw new Exception(String.format("the path:%s of the table:%s does not have any effective file,it is a empty partition.", dbLocalHdfsFilePath, tableInfo.getTableName()));
            }
            return dbDescriptor;
        } catch (Exception e) {
            LOG.error("create tmp db error", e);
            throw e;
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }


    public Object[] query(String primaryFieldVal, HiveSideTableInfo tableInfo) throws Exception {
        if (isClose.get()) {
            LOG.warn("the service has been shut down");
            return null;
        }
        byte[] lineByte = curDb.get(primaryFieldVal);
        if (lineByte == null) {
            return null;
        }
        String lineStr = new String(lineByte);
        Object[] ret = new Object[tableInfo.getPhysicalFields().size()];
        String format = tableInfo.getFormat();
        switch (format) {
            case HiveSideTableInfo.TABLE_FORMAT_TEXT:
                textLine2ObjectArray(lineStr, tableInfo, ret);
                break;
            case HiveSideTableInfo.TABLE_FORMAT_ORC:
                orcLine2ObjectArray(lineStr, tableInfo, ret);
                break;
            default:
                throw new Exception("Currently unsupported table types:" + format);
        }

        return ret;
    }


    private static void textLine2ObjectArray(String lineStr, HiveSideTableInfo tableInfo, Object[] ret) throws Exception {
        String[] strRet = StringUtils.splitByWholeSeparatorPreserveAllTokens(lineStr, tableInfo.getDelimiter());
        for (int i = 0; i < ret.length; i++) {
            if (i < strRet.length) {
                try {
                    ret[i] = ObjectUtil.getTarget(strRet[i], tableInfo.getPhysicalFields().get(i).getType().getClass());
                } catch (Exception e) {
                    throw new Exception(String.format("convert table:%s,filedIndex:%s,fieldValue:%s error", tableInfo.getTableName(), i, strRet[i]), e);
                }
            }
        }
    }


    private static void orcLine2ObjectArray(String lineStr, HiveSideTableInfo tableInfo, Object[] ret) throws Exception {
        JSONArray jsonArray = JSONArray.parseArray(lineStr);
        for (int i = 0; i < ret.length; i++) {
            if (i < jsonArray.size()) {
                try {
                    ret[i] = ObjectUtil.getTarget(jsonArray.get(i), tableInfo.getPhysicalFields().get(i).getType().getClass());
                } catch (Exception e) {
                    throw new Exception(String.format("convert table:%s,filedIndex:%s,fieldValue:%s error", tableInfo.getTableName(), i, jsonArray.get(i)), e);
                }
            }
        }
    }


    private DbDescriptor fullCacheLoadInit() {
        String tableName = tableInfo.getTableName();
        LOG.info("Start to load the latest partition data of the table:{} for the first time", tableName);
        String hdfsPath;
        try {
            /* to get the data ready partition. if the keeper server is unavailable,then throw runtimeException.
             *  if not meet the check condition,hdfsPath is null.
             */
            hdfsPath = HiveMetaWrapper.getReadyPath(tableInfo);
        } catch (Exception e) {
            throw new RuntimeException("init hive side data failure. Call for Keeper#getRecentReadyPartition exception", e);
        }
        DbDescriptor dbDescriptor;
        if (StringUtils.isNotBlank(hdfsPath)) {
            try {
                dbDescriptor = createTmpDB(hdfsPath);
                curHdfsPath = hdfsPath;
                LOG.info("init the latest data ready partition data of the table:{},path:{} successfully."
                        , tableName, hdfsPath);
                return dbDescriptor;
            } catch (Exception e) {
                LOG.error("Fail to load the latest data ready partition hdfs path of the table:{},path:{},wait for next schedule." +
                                "Try load the latest {} partition data"
                        , tableName, hdfsPath, HiveMetaWrapper.RECENT_HIVE_PARTITION_NUM, e);
            }
        }
        // the failover logical
        List<HiveMetaWrapper.HiveRecentPartitionReadyResp.Data.Partition> recentPartitions;
        try {
            recentPartitions = HiveMetaWrapper.getRecentPartitions(tableInfo);
        } catch (Exception e) {
            throw new RuntimeException("init hive side data failure. Call for Keeper#getRecentPartitions exception", e);
        }
        if (CollectionUtils.isNotEmpty(recentPartitions)) {
            LOG.info("The table:{} does not meet the conditions of dqc stand,try to init the latest {} partition"
                    , tableName, HiveMetaWrapper.RECENT_HIVE_PARTITION_NUM);
            for (HiveMetaWrapper.HiveRecentPartitionReadyResp.Data.Partition recentPartition : recentPartitions) {
                try {
                    hdfsPath = HiveMetaWrapper.buildPath(tableInfo, recentPartition);
                    dbDescriptor = createTmpDB(hdfsPath);
                    curHdfsPath = hdfsPath;
                    LOG.info("init the latest partition data of the table:{},path:{} successfully.", tableName, hdfsPath);
                    return dbDescriptor;
                } catch (Exception e) {
                    LOG.error("Fail to init the latest partition hdfs path of the table:{},path:{}"
                            , tableName, hdfsPath, e);
                }
            }
        }
        throw new RuntimeException(String.format("Fail to init the table:%s data to cache", tableName));

    }


    private DbDescriptor fullCacheLoadSchedule() {
        String tableName = tableInfo.getTableName();
        LOG.info("Start scheduling to load the latest partition data of the table:{}", tableName);
        String readyHdfsPath = null;
        try {
            readyHdfsPath = HiveMetaWrapper.getReadyPath(tableInfo);
        } catch (Exception e) {
            LOG.error("Fail to get the latest ready partition hdfs path of the table:{}", tableName, e);
        }

        DbDescriptor dbDescriptor;
        if (StringUtils.isNotBlank(readyHdfsPath)) {
            if (curHdfsPath.equals(readyHdfsPath)) {
                LOG.warn("The table:{},latest data ready partition path is :{},same as the pre loaded path:{},skip this."
                        , tableName, readyHdfsPath, curHdfsPath);
                return null;
            }
            try {
                /* Ensure data quality, if there is an exception will wait for the next load */
                dbDescriptor = createTmpDB(readyHdfsPath);
                curHdfsPath = readyHdfsPath;
                LOG.info("Load the latest data ready partition data of the table:{},path:{} successfully.", tableName, readyHdfsPath);
                return dbDescriptor;
            } catch (Exception e) {
                LOG.error("Fail to load the latest data ready partition hdfs path of the table:{},path:{},wait for next schedule"
                        , tableName, readyHdfsPath, e);
                return null;
            }
        } else {
            if (!tableInfo.isDqcCheck()) {
                LOG.info("While the table:{} does not meet the conditions of dqc stand,try to load the latest {} partition"
                        , tableName, HiveMetaWrapper.RECENT_HIVE_PARTITION_NUM);
                List<HiveMetaWrapper.HiveRecentPartitionReadyResp.Data.Partition> recentPartitions;
                try {
                    // the failover logical
                    recentPartitions = HiveMetaWrapper.getRecentPartitions(tableInfo);
                } catch (Exception e) {
                    LOG.error("Fail to get the latest ready partition from keeper for the table:{}", tableName, e);
                    return null;
                }
                String hdfsPath = null;
                if (CollectionUtils.isNotEmpty(recentPartitions)) {
                    for (HiveMetaWrapper.HiveRecentPartitionReadyResp.Data.Partition recentPartition : recentPartitions) {
                        try {
                            hdfsPath = HiveMetaWrapper.buildPath(tableInfo, recentPartition);
                            if (curHdfsPath.equals(hdfsPath)) {
                                LOG.warn("The table:{},latest partition path is :{},same as the pre loaded path:{},skip this."
                                        , tableName, hdfsPath, curHdfsPath);
                                return null;
                            }
                            dbDescriptor = createTmpDB(hdfsPath);
                            curHdfsPath = hdfsPath;
                            LOG.info("Load the latest partition data of the table:{},path:{} successfully.", tableName, hdfsPath);
                            return dbDescriptor;
                        } catch (Exception e) {
                            LOG.error("Fail to load the latest partition hdfs path of the table:{},path:{}", tableName, hdfsPath, e);
                        }
                    }
                }
            }
        }
        LOG.error("Fail to load the latest partition hdfs path of the table:{},wait for next schedule", tableName);
        return null;
    }


    /**
     * 启动探测线程
     */
    private void startDetector() {
        if (scheduledExecutorService == null) {
            scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        }
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            scheduledExecutorService.scheduleWithFixedDelay(
                    this::trySwitchDb,
                    partitionKeys.size() == 1 ? DAY_TABLE_DETECTOR_PERIOD : HOUR_TABLE_DETECTOR_PERIOD,
                    partitionKeys.size() == 1 ? DAY_TABLE_DETECTOR_PERIOD : HOUR_TABLE_DETECTOR_PERIOD,
                    TimeUnit.MINUTES
            );
        } else {
            scheduledExecutorService.scheduleWithFixedDelay(
                    this::trySwitchDb,
                    DAY_TABLE_DETECTOR_PERIOD,
                    DAY_TABLE_DETECTOR_PERIOD,
                    TimeUnit.MINUTES
            );
        }

    }

    /**
     * 探测线程执行逻辑，加载数据，切换数据
     */
    private void trySwitchDb() {
        DbDescriptor newDB = fullCacheLoadSchedule();
        if (newDB == null) {
            return;
        }
        DbDescriptor oldDB = curDb;
        curDb = newDB;
        destroyDb(oldDB);
    }


    private void destroyDb(DbDescriptor descriptor) {
        try {
            LOG.info("destroyDb closing: {}", descriptor.getClass().getName());
            descriptor.close();
        } catch (Exception e) {
            LOG.error("closing failed: {}", descriptor.getClass().getName());
        }
    }


    private boolean downloadFileFromHdfs(FileSystem fs, String remotePath, String localPath) {
        long start = System.currentTimeMillis();
        LOG.info("start download file from hdfs: {} to local: {}", remotePath, localPath);
        try {
            if (fs.isFile(new Path(remotePath))) {
                LOG.info("remotePath is file:{}, not support", remotePath);
                return false;
            }

            fs.copyToLocalFile(new Path(remotePath), new Path(localPath));
            LOG.info("download file from hdfs: {} to local: {} completed, cost: {} ms", remotePath, localPath, System.currentTimeMillis() - start);
            return true;
        } catch (Exception e) {
            LOG.error("download hdfs file:{} error", remotePath, e);
            FileUtil.delete(localPath, true);
            return false;
        }
    }


    private void stopDetector() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
            scheduledExecutorService = null;
        }
    }

    /**
     * called when TM shutdown
     */
    protected void shutdown() {
        LOG.info("shutdown HiveTableCache.");
        stopDetector();
        if (curDb != null) {
            destroyDb(curDb);
            FileUtils.deleteQuietly(new File(DB_RESOURCE_ROOT));
            LOG.info("delete resource root: {}", DB_RESOURCE_ROOT);
            isClose.set(true);
            curDb = null;
        }
    }


    /**
     * Check whether the partition meets the load conditions.
     *
     * @param tableInfo
     * @throws RuntimeException
     */
    private void validPartition(HiveSideTableInfo tableInfo) {
        String tableName = tableInfo.getTableName();
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        if (CollectionUtils.isEmpty(partitionKeys)) {
            throw new RuntimeException(tableName + " is not a Partition table.");
        }
        if (partitionKeys.size() > 2
                || (partitionKeys.size() == 1 && !partitionKeys.get(0).equals("log_date"))
                || (partitionKeys.size() == 2 && (!partitionKeys.get(0).equals("log_date") || !partitionKeys.get(1).equals("log_hour")))) {
            throw new RuntimeException(tableName + " partition fields does not conform to standard type,only log_date,log_hour are supported as partition fields");
        }
    }


}
