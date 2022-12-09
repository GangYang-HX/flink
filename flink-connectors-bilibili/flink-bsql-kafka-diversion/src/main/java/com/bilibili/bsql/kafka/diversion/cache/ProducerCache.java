package com.bilibili.bsql.kafka.diversion.cache;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase.KEY_DISABLE_METRICS;

public class ProducerCache {

	private final Properties properties;
	private final MetricGroup metricGroup;

	private static final Logger LOG = LoggerFactory.getLogger(ProducerCache.class);
	private final Map<String, KafkaProducer<byte[], byte[]>> cache = new HashMap<>(8);

	public ProducerCache(Properties properties, MetricGroup metricGroup) {
		this.properties = properties;
		this.metricGroup = metricGroup;
	}

	public KafkaProducer<byte[], byte[]> getProducer(String brokers) {
		KafkaProducer<byte[], byte[]> kafkaProducer = cache.get(brokers);
		if (kafkaProducer == null) {
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
			kafkaProducer = new KafkaProducer<>(properties);
			openMetrcis(kafkaProducer, brokers);
			cache.put(brokers, kafkaProducer);
		}
		return kafkaProducer;
	}

	private void openMetrcis(KafkaProducer<byte[], byte[]> producer, String brokers) {
		if (!Boolean.parseBoolean(properties.getProperty(KEY_DISABLE_METRICS, "false"))) {
			Map<MetricName, ? extends Metric> metrics = producer.metrics();
			if (metrics == null) {
				LOG.warn("Producer implementation does not support metrics");
			} else {
				final MetricGroup producerMetricGroup = metricGroup.addGroup("DiversionKafkaProducer", getSingleBroker(brokers));
				for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
					producerMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
				}
			}
		}
	}

	public static String getSingleBroker(String brokers) {
		String singleBroker;
		try {
			singleBroker = brokers.split(",")[0];
		} catch (Exception e) {
			LOG.error("split brokers [{}] error:", brokers);
			singleBroker = "ILLEGAL_BROKER";
		}
		return singleBroker;
	}

	public void closeAll() {
		cache.forEach((key, producer) -> {
			try {
				producer.close();
			} catch (Exception e) {
				LOG.warn("close all producer error", e);
			}
		});
		cache.clear();
	}

	public void close(String brokers) {
		KafkaProducer<byte[], byte[]> kafkaProducer = cache.get(brokers);
		try {
			kafkaProducer.close();
		} catch (Exception e) {
			LOG.warn("close producer error", e);
		}
		cache.remove(brokers);
	}

	public void flushAll() {
		cache.forEach((key, producer) -> {
			try {
				producer.flush();
			} catch (Exception e) {
				LOG.warn("flush all producer error", e);
			}
		});
	}
}
