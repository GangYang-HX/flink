package com.bilibili.bsql.kafka.diversion.producer;

import com.bilibili.bsql.kafka.ability.metadata.write.WritableMetadata;
import com.bilibili.bsql.kafka.diversion.cache.ProducerCache;
import com.bilibili.bsql.kafka.diversion.metrics.DiversionMetricService;
import com.bilibili.bsql.kafka.diversion.metrics.DiversionMetricsWrapper;
import com.bilibili.bsql.kafka.producer.BsqlKafkaProducer;
import com.bilibili.bsql.kafka.sink.HashKeySerializationSchemaWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;

public class DiversionProducer extends BsqlKafkaProducer {

	private static final Logger LOG = LoggerFactory.getLogger(DiversionProducer.class);
	private final String brokers;
	private final boolean singleProducer;
	private transient ProducerCache producerCache;
	private transient DiversionMetricService sinkMetricsWrapper;

	public DiversionProducer(String topicId, String brokers, String brokerUdfClassName, HashKeySerializationSchemaWrapper serializationSchema,
							 Properties producerConfig, @Nullable FlinkKafkaPartitioner<RowData> customPartitioner) {
		super(topicId, serializationSchema, producerConfig, customPartitioner);
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
		if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
			flushOnCheckpoint = false;
		}
		if (logFailuresOnly) {
			callback = (metadata, e) -> {
				if (e != null) {
					LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
				}
				acknowledgeMessage();
			};
		} else {
			callback = (metadata, exception) -> {
				if (exception != null && asyncException == null) {
					asyncException = exception;
				}
				acknowledgeMessage();
			};
		}
		//single produce shoud be assign.
		if (singleProducer) {
			this.producerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
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
		init();
	}

	@Override
	public void init() {
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(SINK_METRICS, "diversionKafka");
		sinkMetricsWrapper = new DiversionMetricsWrapper(metricGroup);
	}

	@Override
	public void invoke(RowData value, Context context) throws Exception {
		long start = System.nanoTime();
		checkErroneous();
		if (value.getRowKind() == RowKind.UPDATE_BEFORE || value.getRowKind() == RowKind.DELETE) {
			return;
		}
		HashKeySerializationSchemaWrapper.kafkaRecord wrapperRecord = wrapper.generateRecord(value);


		byte[] serializedKey = wrapperRecord.key;
		byte[] serializedValue = wrapperRecord.value;

		String targetTopic = schema.getTargetTopic(value);
		if (null == targetTopic) {
			targetTopic = defaultTopicId;
		}
		String actualBrokers;
		ProducerRecord<byte[], byte[]> record;
		int[] partitions;
		if (singleProducer) {
			actualBrokers = brokers;
			partitions = topicPartitionsMap.get(targetTopic);
			if (null == partitions) {
				partitions = getPartitionsByTopic(targetTopic, producer);
				topicPartitionsMap.put(targetTopic, partitions);
			}
		} else {
			actualBrokers = schema.getTargetBrokers(value);
			producer = producerCache.getProducer(actualBrokers);
			partitions = topicPartitionsMap.get(actualBrokers + "|" + targetTopic);
			if (null == partitions) {
				partitions = getPartitionsByTopic(targetTopic, producer);
				topicPartitionsMap.put(actualBrokers + "|" + targetTopic, partitions);
			}
		}
		if (flinkKafkaPartitioner == null) {
			record = new ProducerRecord<>(targetTopic, null, wrapper.readMetaData(value, WritableMetadata.TIMESTAMP),
				serializedKey, serializedValue, wrapper.readMetaData(value, WritableMetadata.HEADERS));
		} else {
			record = new ProducerRecord<>(targetTopic,
				flinkKafkaPartitioner.partition(value, serializedKey, serializedValue, targetTopic, partitions),
				wrapper.readMetaData(value, WritableMetadata.TIMESTAMP), serializedKey, serializedValue,
				wrapper.readMetaData(value, WritableMetadata.HEADERS));
		}
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
		}
		producer.send(record, callback);
		sinkMetricsWrapper.rtWrite(actualBrokers, targetTopic, start);
		sinkMetricsWrapper.writeSuccess(actualBrokers, targetTopic);
	}


	@Override
	public void close() throws Exception {
		if (singleProducer) {
			super.close();
		} else {
			producerCache.closeAll();
			checkErroneous();
		}
	}

	private void acknowledgeMessage() {
		if (this.flushOnCheckpoint) {
			synchronized (this.pendingRecordsLock) {
				--this.pendingRecords;
				if (this.pendingRecords == 0L) {
					this.pendingRecordsLock.notifyAll();
				}
			}
		}
	}
}
