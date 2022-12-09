package com.bilibili.bsql.kafka.util;

import org.apache.flink.util.StringUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/** Util for kafka topic discovery. */
public class KafkaTPDetector {

    public static final Integer DEFAULT_SHARD_NUM = -1;
    public static final String CONSUMER_DETECT_REQUEST_TIMEOUT_MS = "30000";
    public static final String SESSION_TIMEOUT_MS_CONFIG = "4000";

    public static int getTopicTpNumber(Properties kafkaProperties, String... targetTopics) {
        int partitionNum = DEFAULT_SHARD_NUM;
        Properties detectClientProps = (Properties) kafkaProperties.clone();
        detectClientProps.setProperty(
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, CONSUMER_DETECT_REQUEST_TIMEOUT_MS);
        detectClientProps.setProperty(
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS_CONFIG);
        detectClientProps.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        detectClientProps.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int retryCount = 0;

        while (true) {
            try (KafkaConsumer consumer = new KafkaConsumer(detectClientProps)) {
                for (String targetTopic : targetTopics) {
                    int currentTopicTPNumber = consumer.partitionsFor(targetTopic).size();
                    if (partitionNum == DEFAULT_SHARD_NUM || partitionNum > currentTopicTPNumber) {
                        partitionNum = currentTopicTPNumber;
                    }
                }
                return partitionNum;
            } catch (Exception e) {
                if (retryCount++ > 3) {
                    throw new RuntimeException(
                            "Unable to obtain the information about the topic:"
                                    + StringUtils.toQuotedListString(targetTopics)
                                    + "corresponding to the source/sink table,error info:"
                                    + e.getMessage());
                }
            }
        }
    }
}
