/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.producer;

import com.bilibili.bsql.kafka.sink.HashKeySerializationSchemaWrapper;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Shin
 * @version $Id: DiscoverKafkaProducer.java, v 0.1 2021-08-02 11:18
 */
@PublicEvolving
public class DiscoverKafkaProducer<T> extends FlinkKafkaProducer010<T> {

	private static final long serialVersionUID = 1L;
	protected Map<String, int[]> TempMap;
	protected static volatile ScheduledExecutorService scheduledExecutorService;
	private final static Logger LOG = LoggerFactory.getLogger(DiscoverKafkaProducer.class);
	protected HashKeySerializationSchemaWrapper wrapper;

	// ---------------------- Regular constructors------------------

	public DiscoverKafkaProducer(
		String topicId,
		SerializationSchema<T> serializationSchema,
		Properties producerConfig,
		@Nullable FlinkKafkaPartitioner<T> customPartitioner) {
		super(topicId, serializationSchema, producerConfig, customPartitioner);
		this.TempMap = new HashMap<>();
	}

	// ------------------- Key/Value serialization schema constructors ----------------------
	public DiscoverKafkaProducer(
		String topicId,
		KeyedSerializationSchema<T> serializationSchema,
		Properties producerConfig,
		@Nullable FlinkKafkaPartitioner<T> customPartitioner) {
		super(topicId, serializationSchema, producerConfig, customPartitioner);
		this.wrapper = (HashKeySerializationSchemaWrapper) serializationSchema;
		this.TempMap = new HashMap<>();
	}

	// ------------------- User configuration ----------------------


	public synchronized static ScheduledExecutorService getInstance() {
		if (scheduledExecutorService == null) {
			synchronized (ScheduledExecutorService.class) {
				if (scheduledExecutorService == null) {
					scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
				}
			}
		}
		return scheduledExecutorService;
	}

	public void runPartitionDiscover(int initialDelay, int commonDelay) {
		scheduledExecutorService = getInstance();
		scheduledExecutorService.scheduleWithFixedDelay(() -> {
			try {
				for (String topic : topicPartitionsMap.keySet()) {
					TempMap.put(topic, getPartitionsByTopic(topic,producer));
				}
				if(!TempMap.isEmpty()){
					topicPartitionsMap = TempMap;
				}
			} catch (Exception e) {
				LOG.error("get partition error", e);
			}
		}, initialDelay, commonDelay, TimeUnit.SECONDS);
	}


	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		runPartitionDiscover(60, 60);
	}
}
