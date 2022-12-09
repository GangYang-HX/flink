package com.bilibili.bsql.kafka.diversion.metrics;

public interface DiversionMetricService {

	void writeSuccess(String broker, String topic);

	void writeFailed(String broker, String topic);

	void rtWrite(String broker, String topic, long start);

	void retryCount(String broker, String topic);


}
