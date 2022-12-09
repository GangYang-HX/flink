package com.bilibili.bsql.hdfs.cache;

import com.bilibili.bsql.common.global.SymbolsConstant;
import com.bilibili.bsql.common.utils.DateReplaceUtil;
import com.bilibili.bsql.common.utils.FileUtil;
import com.bilibili.bsql.common.utils.ObjectUtil;
import com.bilibili.bsql.hdfs.cache.memory.MemoryDbDescriptor;
import com.bilibili.bsql.hdfs.tableinfo.HdfsSideTableInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HdfsTableCache.java
 * @description This is the description of HdfsTableCache.java
 * @createTime 2020-10-22 21:20:00
 */
public class HdfsTableCache {
	private final static Logger LOG = LoggerFactory.getLogger(HdfsTableCache.class);
	private final HdfsSideTableInfo tableInfo;
	private String DB_RESOURCE_ROOT;
	private volatile DbDescriptor curDb;
	private AtomicBoolean isClose;
	private int keyIndex;
	private Scheduler scheduler;
	private ScheduledExecutorService scheduledExecutorService;
	private final static String CRON_EXPRESSION = "0 0 %s * * ?";
	private final static String FLINK_HDFS_SIDE = "flink-hdfs-side";
	private final static int DETECTOR_PERIOD = 5;
	private static String localDirs;


	protected HdfsTableCache(HdfsSideTableInfo tableInfo) throws Exception {
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
		// 目前只支持单字段的key
		String keyField = tableInfo.getPrimaryKeys().get(0);
		String[] allFields = tableInfo.getFieldNames();
		for (int i = 0; i < allFields.length; i++) {
			if (StringUtils.equalsIgnoreCase(keyField, allFields[i])) {
				keyIndex = i;
				break;
			}
		}
		init();
	}

	public Object[] query(String primaryFieldVal) throws Exception {
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
		String[] strRet = StringUtils.splitByWholeSeparatorPreserveAllTokens(lineStr, tableInfo.getDelimiter());
		for (int i = 0; i < ret.length; i++) {
			if (i < strRet.length) {
				try {
					ret[i] = ObjectUtil.getTarget(strRet[i], tableInfo.getPhysicalFields().get(i).getType().getClass());
				} catch (Exception e) {
					LOG.warn("convert type error,val{}", strRet[i], e);
				}
			}
		}
		return ret;
	}


	/**
	 * 注意，此处逻辑的重点在于2个判断逻辑：是否初始化、维表数据是否最新；以及3个条件：createTmpDB成功且维表最新、createTmpDB成功但维表非最新、createTmpDB失败抛异常；
	 * 所以会由isInit条件和exception来决定代码逻辑走向
	 * <p>
	 * isInit == true: 初始化DB
	 * 有failover逻辑，尽最大可能初始化DB
	 * 如果初始化成功，但是维表数据不是最新，则启动探测器实现最新数据的探测。直到探测成功，或者下一次UpdateJob开始
	 * <p>
	 * isInit == false: 更新DB
	 * 只关注最新的维表数据，没有failover逻辑。
	 * 如果新DB没有构建成功，则启动探测。直到探测成功，或者下一次UpdateJob开始
	 *
	 * @return new db
	 */
	private DbDescriptor createDbWithDetector(boolean isInit) {
		String hdfsFilePath = DateReplaceUtil.replaceDateExpression(tableInfo.getPath(), 0);
		try {
			return createTmpDB(hdfsFilePath);
		} catch (Exception e) {
			//ignore
		}

		if (isInit) {
			for (int i = 1; i <= tableInfo.getFailoverTimes(); i++) {
				String tmpPath = DateReplaceUtil.replaceDateExpression(tableInfo.getPath(), i);
				try {
					DbDescriptor ret = createTmpDB(tmpPath);
					startDetector();
					return ret;
				} catch (Exception e) {
					//ignore
				}
			}
			throw new RuntimeException("no valid hdfs path");
		} else {
			startDetector();
			return null;
		}
	}

	private void stopDetector() {
		if (scheduledExecutorService != null) {
			scheduledExecutorService.shutdownNow();
			scheduledExecutorService = null;
		}
	}

	private void startDetector() {
		if (scheduledExecutorService == null) {
			scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
		}
		scheduledExecutorService.schedule(this::trySwitchDb, DETECTOR_PERIOD, TimeUnit.MINUTES);
	}


	private synchronized void init() throws Exception {
		initResourceRoot();
		curDb = createDbWithDetector(true);

		isClose = new AtomicBoolean(false);
		startUpdateThread();
	}

	private void initResourceRoot() {
		LOG.info("java.io.tmpdir: {}", localDirs);
		if (StringUtils.isNotBlank(localDirs)) {
			DB_RESOURCE_ROOT = localDirs + "/" + HdfsTableCache.FLINK_HDFS_SIDE + "-" + UUID.randomUUID() + "/";
			LOG.info("resource root tmp path: {}", DB_RESOURCE_ROOT);
		} else {
			throw new RuntimeException("the environment variable java.io.tmpdir directory is empty.");
		}
	}

	private void trySwitchDb() {
		DbDescriptor newDB = createDbWithDetector(false);
		if (newDB == null) {
			return;
		}
		DbDescriptor oldDB = curDb;
		curDb = newDB;
		destroyDb(oldDB);
	}

	public void triggerUpdateDb() {
		stopDetector();
		trySwitchDb();
	}

	private DbDescriptor createTmpDB(String remotePath) throws Exception {
		FileSystem fs = null;
		try {
			Configuration conf = new Configuration();
			fs = FileSystem.newInstance(FileSystem.getDefaultUri(conf), conf, this.tableInfo.getHdfsUser());
			FileStatus[] fileStatus = validPath(fs, remotePath, tableInfo.isCheckSuccessFile());
			// 临时目录名称,根目录+文件名称+pid
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

			//double check success file，if success file not exist，maybe path has been overwrite.should throw exception
			isSuccessCheck(fs, remotePath, tableInfo.isCheckSuccessFile(), fileStatus);

			DbDescriptor dbDescriptor = DbDescriptorFactory.getDbDescriptor(dbParent, uuid, dbLocalHdfsFilePath, tableInfo.getMemoryThresholdSize(), tableInfo.getDelimiter(), keyIndex);
			boolean handleResult = dbDescriptor.loadData2Db(dbLocalHdfsFilePath);
			if (!handleResult && MemoryDbDescriptor.class.getName().equals(dbDescriptor.getClass().getName())) {
				// 假如内存加载失败改用rocksdb
				dbDescriptor = DbDescriptorFactory.getRocksDbDescriptor(dbParent, uuid, tableInfo.getDelimiter(), keyIndex);
				dbDescriptor.loadData2Db(dbLocalHdfsFilePath);
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

	/**
	 * 检验path是否符合load条件
	 *
	 * @param fs
	 * @param remotePath
	 * @return
	 * @throws IOException
	 */
	private FileStatus[] validPath(FileSystem fs, String remotePath, boolean checkSuccessTag) throws IOException {
		if (!fs.exists(new Path(remotePath))) {
			throw new RuntimeException(remotePath + " not exists.");
		}
		if (checkSuccessTag) {
			FileStatus[] fileStatuses = fs.listStatus(new Path(remotePath));
			boolean successTag = fs.exists(new Path(remotePath, "_SUCCESS"))
				|| fs.exists(new Path(remotePath, ".SUCCESS"));
			if (successTag) {
				return fileStatuses;
			} else {
				throw new RuntimeException(remotePath + " SUCCESS file not Exist.");
			}
		}
		return null;
	}

	private void isSuccessCheck(FileSystem fs, String remotePath, boolean checkSuccessTag,
								   FileStatus[] fileStatuses) throws IOException {
		FileStatus[] newFileStatuses = validPath(fs, remotePath, checkSuccessTag);
		if (!checkSuccessTag) {
			return;
		}
		if (null == fileStatuses || null == newFileStatuses) {
			throw new RuntimeException(remotePath + " check SUCCESS file failed caused by file statuses is null.");
		}
		FileStatus f1 = null;
		for (FileStatus fileStatus : fileStatuses) {
			if (fileStatus.getPath().toString().endsWith(".SUCCESS") ||
				fileStatus.getPath().toString().endsWith("_SUCCESS")) {
				f1 = fileStatus;
			}
		}
		FileStatus f2 = null;
		for (FileStatus fileStatus : newFileStatuses) {
			if (fileStatus.getPath().toString().endsWith(".SUCCESS") ||
				fileStatus.getPath().toString().endsWith("_SUCCESS")) {
				f2 = fileStatus;
			}
		}
		if (null != f1 && null != f2 && f1.getPath().equals(f2.getPath())
			&& f1.getModificationTime() == f2.getModificationTime()) {
			LOG.info("success file check passed, before path :{} , modify time:{}, after path :{}, modify time :{}",
				f1.getPath(), f1.getModificationTime(), f2.getPath(), f2.getModificationTime());
		}else {
			throw new RuntimeException(remotePath + " check SUCCESS file failed caused by file statuses not equal.");
		}
	}

	private void startUpdateThread() throws SchedulerException {
		SchedulerFactory schedulerFactory = new StdSchedulerFactory();
		scheduler = schedulerFactory.getScheduler();
		JobDetail job = JobBuilder.newJob(UpdateJob.class).build();
		job.getJobDataMap().put(UpdateJob.UPDATE_CACHE, this);
		String period = tableInfo.getPeriod();
		if (StringUtils.isNotEmpty(period)) {
			String periodHour = period.split(SymbolsConstant.COLON)[1];
			String cron = String.format(CRON_EXPRESSION, periodHour);
			// String cron = "0 0/1 * * * ?";
			// 周期更新hive维表数据
			Trigger trigger = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(cron)).startAt(new Date()).build();
			scheduler.scheduleJob(job, trigger);
			scheduler.start();
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

	/**
	 * called when TM shutdown
	 */
	protected void shutdown() {
		LOG.info("shutdown HdfsTableCache.");
		if (scheduler != null) {
			try {
				scheduler.shutdown(true);
				scheduler = null;
			} catch (Exception e) {
				// ignore
			}
		}
		if (curDb != null) {
			destroyDb(curDb);
			FileUtils.deleteQuietly(new File(DB_RESOURCE_ROOT));
			LOG.info("delete resource root: {}", DB_RESOURCE_ROOT);
			isClose.set(true);
			curDb = null;
		}
	}

	private void destroyDb(DbDescriptor descriptor) {
		try {
			LOG.info("destroyDb closing: {}", descriptor.getClass().getName());
			descriptor.close();
		} catch (Exception e) {
			LOG.error("closing failed: {}", descriptor.getClass().getName());
		}
	}

}
