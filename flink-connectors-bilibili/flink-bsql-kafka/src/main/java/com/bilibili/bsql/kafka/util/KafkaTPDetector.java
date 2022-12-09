/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.util;

import java.util.Properties;

import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.CONSUMER_DETECT_REQUEST_TIMEOUT_MS;
import static org.apache.flink.table.connector.ConnectorValues.DEFAULT_SHARD_NUM;

/**
 *
 * @author zhouxiaogang
 * @version $Id: KafkaTPDetector.java, v 0.1 2021-01-12 11:46
zhouxiaogang Exp $$
 */
public class KafkaTPDetector {

	public static int getTopicTpNumber(Properties kafkaProperties, String... targetTopics) {
		int partitionNum = DEFAULT_SHARD_NUM;

		Properties detectClientProps = (Properties)kafkaProperties.clone();

		// 设置5s超时
		detectClientProps.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
			detectClientProps.getProperty(CONSUMER_DETECT_REQUEST_TIMEOUT_MS.key()));
		detectClientProps.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "4000");
		detectClientProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		detectClientProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		int retryCount = 0;

		while (true) {
			try (KafkaConsumer consumer = new KafkaConsumer(detectClientProps)) {
				for (String targetTopic : targetTopics) {
					int currentTopicTPNumber = consumer.partitionsFor(targetTopic).size();
					if (partitionNum ==DEFAULT_SHARD_NUM ||  partitionNum > currentTopicTPNumber) {
						partitionNum = currentTopicTPNumber;
					}
				}
				return partitionNum;
			} catch (Exception e) {
				if (retryCount++ > 3) {
					throw new RuntimeException("无法获取source/sink表对应topic:" + StringUtils.toQuotedListString(targetTopics)
						+ "的信息,错误:" + e.getMessage());
				}
			}
		}
	}
}
