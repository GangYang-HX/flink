package com.bilibili.bsql.kafka.diversion.producer;

import com.bilibili.bsql.kafka.producer.RetryDataWithCount;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DiversionRetryDataWithCount extends RetryDataWithCount {

	public KafkaProducer<byte[], byte[]> producer;
	public String brokers;

	public DiversionRetryDataWithCount(String brokers, KafkaProducer<byte[], byte[]> producer,
									   ProducerRecord<byte[], byte[]> record, int failedTimes) {
		super(record, failedTimes);
		this.brokers = brokers;
		this.producer = producer;
	}
}
