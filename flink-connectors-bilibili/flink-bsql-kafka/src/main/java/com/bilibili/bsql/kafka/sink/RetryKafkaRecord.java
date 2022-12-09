package com.bilibili.bsql.kafka.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/** Retry Kafka Record. */
public class RetryKafkaRecord {
    public KafkaProducer<byte[], byte[]> producer;
    public ProducerRecord<byte[], byte[]> record;
    public String brokers;
    public int failedTimes;

    public RetryKafkaRecord(
            KafkaProducer<byte[], byte[]> producer,
            @Nullable String brokers,
            ProducerRecord<byte[], byte[]> record,
            int failedTimes) {
        this.producer = producer;
        this.brokers = brokers;
        this.record = record;
        this.failedTimes = failedTimes;
    }
}
