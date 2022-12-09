package com.bilibili.bsql.hdfs.cache;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className UpdateJob.java
 * @description This is the description of UpdateJob.java
 * @createTime 2020-10-22 21:25:00
 */
public class UpdateJob implements Job {
	private final static Logger LOG = LoggerFactory.getLogger(UpdateJob.class);
	public static final String UPDATE_CACHE = "UPDATE_CACHE";

	@Override
	public void execute(JobExecutionContext jobExecutionContext) {
		try {
			LOG.info("scheduler quartz job");
			HdfsTableCache cache = (HdfsTableCache) jobExecutionContext.getJobDetail().getJobDataMap().get(UPDATE_CACHE);
			cache.triggerUpdateDb();
		} catch (Exception e) {
			LOG.error("update db error", e);
		}
	}
}
