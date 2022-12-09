package org.apache.flink.bili.external.archer.constant;

import java.time.format.DateTimeFormatter;

/**
 * @author: zhuzhengjun
 * @date: 2021/12/20 9:04 下午
 */
public class ArcherConstants {
	/**
	 *  鉴权相关
	 */
	public static final String APP_ID = "datacenter.archer.archer-bypass";
	public static final String SECRET_KEY = "8bd7931b92afcbb189df55c08ef3ca8d";
//	public static final String SECRET_KEY = "5fe71b35457f10fc30ac583206cf8d28";
	public static final String URL = "http://berserker.bilibili.co/voyager/v1/invocation/grpc";
//	public static final String URL = "http://uat-berserker.bilibili.co/voyager/v1/invocation/grpc";
	public static final String ACCOUNT = "p_flink_hdfsSink";
//	public static final String ACCOUNT = "p_archer";
	public static final String USER = "archer_lancer_dummy";

	/**
	 * saber系统相关
	 */
	public static final String SYSTEM_USER_ID = "hdfs.partition.commit.user";
	public static final String LANCER_LOG_ID = "lancer.log.id";

	/**
	 * http api 接口选举
	 */
	public static final String GROUP_NAME = "JobManager";
	public static final String INSTANCE_GROUP_NAME = "InstanceManager";
	public static final String API_CREATE_JOB = "createJob";
	public static final String API_GET_JOB_ID_BY_RELATE_UID = "getJobIdByRelateUID";
	public static final String API_QUERY_INSTANCE = "queryInstance";
	public static final String API_QUERY_INSTANCE_BY_RELATE_UID = "queryInstanceByRelateUId";
	public static final String API_SUCCESS_INSTANCE = "dummyJobExecSuccess";


	/**
	 * http api 参数
	 */
	public static final String JOB_CRON_LOG_HOUR = "0 0 */1 * * ?";
	public static final String JOB_CRON_LOG_DAY = "15 0 0 * * ?";

	/**
	 * time pattern 相关
	 */
	public static final String DAY_PATTERN = "yyyyMMdd";
	public static final String HOUR_PATTERN = "yyyyMMdd/HH";
	public static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern(DAY_PATTERN);
	public static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern(HOUR_PATTERN);

	/**
	 * flink配置相关
	 */
	public static final String SINK_DATABASE_NAME_KEY = "sink.database.name";
	public static final String SINK_TABLE_NAME_KEY = "sink.table.name";
	public static final String SINK_PARTITION_KEY = "sink.partition.key";




}
