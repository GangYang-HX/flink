package com.bilibili.bsql.kafka.diversion.metrics;

public class DiversionMetricsConstant {
	//group
	public static final String DIVERSION_PRODUCER = "diversionProducer";
	public static final String DIVERSION_TOPIC = "diversionTopic";


	//metrics
	public static final String WRITE_SUCCESS_RECORD = "WriteSuccess";
	public static final String WRITE_FAILURE_RECORD = "WriteFailure";
	public static final String RT_WRITE_RECORD = "RtWrite";
	public static final String RETRY_RECORD = "retryCount";

}
