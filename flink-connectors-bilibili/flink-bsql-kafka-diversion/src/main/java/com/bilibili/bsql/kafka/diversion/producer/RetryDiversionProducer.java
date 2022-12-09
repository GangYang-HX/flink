package com.bilibili.bsql.kafka.diversion.producer;

import com.bilibili.bsql.kafka.ability.metadata.write.WritableMetadata;
import com.bilibili.bsql.kafka.blacklist.ProducerBlacklistTpUtils;
import com.bilibili.bsql.kafka.diversion.cache.ProducerCache;
import com.bilibili.bsql.kafka.diversion.metrics.DiversionMetricService;
import com.bilibili.bsql.kafka.diversion.metrics.DiversionRetryMetricsWrapper;
import com.bilibili.bsql.kafka.producer.RetryKafkaProducer;
import com.bilibili.bsql.kafka.sink.HashKeySerializationSchemaWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class RetryDiversionProducer extends RetryKafkaProducer {

	private static final Logger LOG = LoggerFactory.getLogger(RetryDiversionProducer.class);
	private final String brokers;
	private final boolean singleProducer;
	private transient ProducerCache producerCache;
	private transient DiversionMetricService sinkMetricsWrapper;
	private transient LinkedBlockingQueue<DiversionRetryDataWithCount> retryQueue;
    /**
     * blacklist tp relay topicPartitionMap's key to observe blacklist tp in zookeeper, but if RetryDiversionProducer
     * enable multi producer, topicPartitionMap's key is <broker | topic>, it will lead blacklist tp function lose efficacy,
     * so we use allTps to save <topic, partition>
     */
    private Map<String, int[]> allTps = new HashMap<>();

	public RetryDiversionProducer(String topicId, String brokers, String brokerUdfClassName,
								  HashKeySerializationSchemaWrapper serializationSchema, Properties producerConfig,
								  @Nullable FlinkKafkaPartitioner<RowData> customPartitioner, Integer timeoutRetryTimes) {
		super(topicId, serializationSchema, producerConfig, customPartitioner, timeoutRetryTimes);
		this.brokers = brokers;
		this.singleProducer = StringUtils.isNullOrWhitespaceOnly(brokerUdfClassName);
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		if (schema instanceof KeyedSerializationSchemaWrapper) {
			((KeyedSerializationSchemaWrapper<RowData>) schema).getSerializationSchema()
				.open(() -> getRuntimeContext().getMetricGroup().addGroup("user"));
		}
		if (null != flinkKafkaPartitioner) {
			flinkKafkaPartitioner.open(getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks());
		}
		//single produce shoud be assign.
		if (singleProducer) {
			producer = getKafkaProducer(this.producerConfig);
			if (!Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
				Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

				if (metrics == null) {
					// MapR's Kafka implementation returns null here.
					LOG.info("Producer implementation does not support metrics");
				} else {
					final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaProducer");
					for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
						kafkaMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
					}
				}
			}
		} else {
			producerCache = new ProducerCache(producerConfig, getRuntimeContext().getMetricGroup());
		}
		retryQueue = new LinkedBlockingQueue<>();
		retryingPendingRecords = new AtomicLong();
		MaxSnapshotRetry = 0L;
		RetryRecords = 0L;
		getRuntimeContext().getMetricGroup().gauge(
			"kafkaRetryingRecord",
			(Gauge<Long>) () -> retryingPendingRecords.get()
		);

		getRuntimeContext().getMetricGroup().gauge(
			"kafkaMaxSnapshotRetry",
			(Gauge<Long>) () -> MaxSnapshotRetry
		);

		retryPool = Executors.newSingleThreadExecutor();
		resetPool = new ScheduledThreadPoolExecutor(1);

		resetPool.scheduleWithFixedDelay(() -> retryPartitioner.resetPartition(), 1, 1, TimeUnit.MINUTES);
		retryPool.submit((Runnable) () -> {
			while (true) {
				try {
					DiversionRetryDataWithCount retryRecord = retryQueue.take();
					int failedTimes = retryRecord.failedTimes;
					ProducerRecord<byte[], byte[]> record = retryRecord.record;
					KafkaProducer<byte[], byte[]> retryProducer = retryRecord.producer;
					String recordBrokers = retryRecord.brokers;
					byte[] serializedKey = record.key();
					byte[] serializedValue = record.value();
					Long timestamp = record.timestamp();
					Headers headers = record.headers();
					String targetTopic = record.topic();
					int[] partitions;
					if (singleProducer) {
						partitions = topicPartitionsMap.computeIfAbsent(targetTopic, f -> {
							int[] part = getPartitionsByTopic(targetTopic, retryProducer);
							topicPartitionsMap.put(targetTopic, part);
							// record topic' partition for blacklist
                            allTps.put(targetTopic, part);
                            return part;
						});
					} else {
						partitions = topicPartitionsMap.computeIfAbsent(recordBrokers + "|" + targetTopic, f -> {
							int[] part = getPartitionsByTopic(targetTopic, retryProducer);
							topicPartitionsMap.put(recordBrokers + "|" + targetTopic, part);
                            allTps.put(targetTopic, part);
                            return part;
						});
					}
					ProducerRecord<byte[], byte[]> newRecord = new ProducerRecord<>(
						targetTopic,
						retryPartitioner.partition(
							new GenericRowData(0),
							serializedKey,
							serializedValue,
							targetTopic,
							partitions
						),
						timestamp, serializedKey,
						serializedValue, headers
					);
					retryProducer.send(newRecord, new DiversionRetryCallBack(retryProducer, recordBrokers, newRecord, failedTimes));
				} catch (InterruptedException e) {
					LOG.error("kafka sink retry take from queue error", e);
				} catch (Exception e) {
					LOG.error("kafka sink retry error", e);
				}
			}
		});
		init();
	}

	@Override
	public void init() {
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(SINK_METRICS, "diversionKafka");
		sinkMetricsWrapper = new DiversionRetryMetricsWrapper(metricGroup);
        if (Boolean.parseBoolean(this.producerConfig.getProperty(ProducerBlacklistTpUtils.BLACKLIST_ENABLE))){
            super.startAggScheduler(this.allTps);
        }
	}

	@Override
	public void invoke(RowData value, Context context) throws Exception {
		while (retryingPendingRecords.get() > 5_000_000) {
			LOG.info("kafka retry sink has too mush retrying records, slowing the process down");
			Thread.sleep(5000L);
		}

		long start = System.nanoTime();
		if (value.getRowKind() == RowKind.UPDATE_BEFORE || value.getRowKind() == RowKind.DELETE) {
			return;
		}
		String actualBrokers;
		String targetTopic = schema.getTargetTopic(value);
		if (null == targetTopic) {
			targetTopic = defaultTopicId;
		}
		int[] partitions;
		if (singleProducer) {
			actualBrokers = brokers;
			partitions = topicPartitionsMap.get(targetTopic);
			if (null == partitions) {
				partitions = getPartitionsByTopic(targetTopic, producer);
				topicPartitionsMap.put(targetTopic, partitions);
                allTps.put(targetTopic, partitions);
            }
		} else {
			actualBrokers = schema.getTargetBrokers(value);
			producer = producerCache.getProducer(actualBrokers);
			partitions = topicPartitionsMap.get(actualBrokers + "|" + targetTopic);
			if (null == partitions) {
				partitions = getPartitionsByTopic(targetTopic, producer);
				topicPartitionsMap.put(actualBrokers + "|" + targetTopic, partitions);
                allTps.put(targetTopic, partitions);
            }
		}
		try {
			HashKeySerializationSchemaWrapper.kafkaRecord wrapperRecord = wrapper.generateRecord(value);
			byte[] serializedKey = wrapperRecord.key;
			byte[] serializedValue = wrapperRecord.value;
			ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
				targetTopic,
				retryPartitioner.partition(value, serializedKey, serializedValue, targetTopic, partitions),
				wrapper.readMetaData(value, WritableMetadata.TIMESTAMP), serializedKey,
				serializedValue, wrapper.readMetaData(value, WritableMetadata.HEADERS)
			);
			producer.send(record, new DiversionFirstCallBack(producer, actualBrokers, record, 0));
			sinkMetricsWrapper.rtWrite(actualBrokers, targetTopic, start);
		} catch (Exception e) {
			sinkMetricsWrapper.writeFailed(actualBrokers, targetTopic);
			throw new Exception("Failed to write kafka record.", e);
		}
		sinkMetricsWrapper.writeSuccess(actualBrokers, targetTopic);
	}

	@Override
	protected void flush() {
		if (singleProducer) {
			super.flush();
		} else {
			producerCache.flushAll();
		}
	}

	@Override
	public void close() throws Exception {
		if (singleProducer) {
			super.close();
		} else {
			producerCache.closeAll();
		}
	}

	public class DiversionFirstCallBack extends FirstCallBack {
		private final KafkaProducer<byte[], byte[]> producer;
		private final String brokers;

		public DiversionFirstCallBack(KafkaProducer<byte[], byte[]> producer, String brokers,
									  ProducerRecord<byte[], byte[]> record, int failedTimes) {
			super(record, failedTimes);
			this.producer = producer;
			this.brokers = brokers;
		}

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			if (exception != null) {
				handleException(exception);
				try {
					RetryDiversionProducer.this.retryPartitioner.addFailedPartition(record.partition());
					retryingPendingRecords.incrementAndGet();
					failedTimes++;
					RetryDiversionProducer.this.sinkMetricsWrapper.retryCount(brokers, record.topic());
					RetryDiversionProducer.this.retryQueue.put(
						new DiversionRetryDataWithCount(brokers, producer, record, failedTimes)
					);
				} catch (InterruptedException e) {
					LOG.error("kafka sink put into retry queue error", e);
				}
			}
		}
	}

	public class DiversionRetryCallBack extends DiversionFirstCallBack {

		public DiversionRetryCallBack(KafkaProducer<byte[], byte[]> producer, String brokers,
									  ProducerRecord<byte[], byte[]> record, int failedTimes) {
			super(producer, brokers, record, failedTimes);
		}

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			if (exception != null) {
				try {
					RetryDiversionProducer.this.retryPartitioner.addFailedPartition(record.partition());
					failedTimes++;
					RetryDiversionProducer.this.retryQueue.put(
						new DiversionRetryDataWithCount(brokers, producer, record, failedTimes)
					);
				} catch (InterruptedException e) {
					LOG.error("kafka retry put into queue error", e);
				}
			} else {
				if (failedTimes != 0) {
					acknowledgeRetryMessage();
				}
			}
		}
	}
}
