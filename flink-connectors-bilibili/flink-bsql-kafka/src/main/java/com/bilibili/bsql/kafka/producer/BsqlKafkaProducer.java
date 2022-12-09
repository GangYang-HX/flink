/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.producer;

import java.util.Properties;

import javax.annotation.Nullable;

import com.bilibili.bsql.kafka.ability.metadata.write.WritableMetadata;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.bilibili.bsql.common.metrics.SinkMetricsWrapper;
import com.bilibili.bsql.kafka.sink.HashKeySerializationSchemaWrapper;

import static com.bilibili.bsql.common.metrics.Metrics.WINDOW_SIZE;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlKafkaProducer.java, v 0.1 2020-10-21 19:46
zhouxiaogang Exp $$
 */
@Slf4j
public class BsqlKafkaProducer extends DiscoverKafkaProducer<RowData> {

	protected static final String SINK_METRICS = "SinkMetrics";

	/**
	 * Counter name to metric success write.
	 */
	private static final String WRITE_SUCCESS_RECORD = "WriteSuccessRecord";

	/**
	 * Counter name to metric success size.
	 */
	private static final String WRITE_SUCCESS_SIZE = "WriteSuccessSize";

	/**
	 * Counter name to metric failed write.
	 */
	private static final String WRITE_FAILURE_RECORD = "WriteFailureRecord";

	/**
	 * Histogram name to write rt.
	 */
	private static final String RT_WRITE_RECORD = "RtWriteRecord";

	protected HashKeySerializationSchemaWrapper wrapper;

	protected SinkMetricsWrapper sinkMetricsGroup;

	/**
	 * trace metric name.
	 */
    protected static final String CREATE_TIME = "ctime";

    protected static final String REQUEST_TIME = "request_time";

    protected static final String UPDATE_TIME = "update_time";

    protected static final String TRACE_ID = "traceId";

    protected static final String TRACE_KIND = "traceKind";

    protected static final String CUSTOM_TRACE_CLASS = "custom.trace.class";

    protected static final String IS_KAFKA_OVER_SIZE_RECORD_IGNORE = "isKafkaRecordOversizeIgnore";

	public BsqlKafkaProducer(String topicId,
							 HashKeySerializationSchemaWrapper serializationSchema,
							 Properties producerConfig,
							 @Nullable FlinkKafkaPartitioner<RowData> customPartitioner) {
		super(topicId, serializationSchema, producerConfig, customPartitioner);
		this.wrapper = serializationSchema;

	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		init();
	}

	public void init() {
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(SINK_METRICS, "kafka");
		sinkMetricsGroup = new SinkMetricsWrapper()
				.setSuccessfulWrites(metricGroup.counter(WRITE_SUCCESS_RECORD))
				.setFailedWrites(metricGroup.counter(WRITE_FAILURE_RECORD))
			    .setSuccessfulSize(metricGroup.counter(WRITE_SUCCESS_SIZE))
				.setWriteRt(metricGroup.histogram(RT_WRITE_RECORD, new DescriptiveStatisticsHistogram(WINDOW_SIZE)));
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
		ProducerRecord<byte[], byte[]> record;
		int[] partitions = topicPartitionsMap.get(targetTopic);
		if (null == partitions) {
			partitions = getPartitionsByTopic(targetTopic, producer);
			topicPartitionsMap.put(targetTopic, partitions);
		}
		if (flinkKafkaPartitioner == null) {
			record = new ProducerRecord<>(targetTopic, null, wrapper.readMetaData(value, WritableMetadata.TIMESTAMP),
				serializedKey, serializedValue, wrapper.readMetaData(value, WritableMetadata.HEADERS));
		} else {
			record = new ProducerRecord<>(targetTopic, flinkKafkaPartitioner.partition(value, serializedKey, serializedValue, targetTopic, partitions),
				wrapper.readMetaData(value, WritableMetadata.TIMESTAMP), serializedKey, serializedValue,
				wrapper.readMetaData(value, WritableMetadata.HEADERS));
		}
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
		}
		producer.send(record, callback);
		sinkMetricsGroup.rtWriteRecord(start);
		sinkMetricsGroup.writeSuccessRecord();
		sinkMetricsGroup.writeSuccessSize(getMessageSize(serializedKey, serializedValue));
	}

	@Override
	protected void flush() {
		if (this.producer != null) {
			producer.flush();
		}
	}

	protected long getMessageSize(byte[] serializedKey, byte[] serializedValue) {
		long size = 0L;
		if (serializedKey != null) {
			size += serializedKey.length;
		}
		if (serializedValue != null) {
			size += serializedValue.length;
		}
		return size;
	}

}
