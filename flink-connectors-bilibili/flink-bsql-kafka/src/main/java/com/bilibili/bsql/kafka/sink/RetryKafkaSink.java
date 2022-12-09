package com.bilibili.bsql.kafka.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.sink.KafkaWriter;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * retry kafka sink.
 *
 * @param <IN>
 */
@PublicEvolving
public class RetryKafkaSink<IN> extends KafkaSink<IN> {
    protected FlinkKafkaPartitioner<IN> partitioner;
    protected Boolean retry;
    protected Integer pendingRetryRecordsLimit;
    protected Properties userDefinedProps;

    public RetryKafkaSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            FlinkKafkaPartitioner<IN> partitioner,
            Boolean retry,
            Integer pendingRetryRecordsLimit,
            Properties userDefinedProps) {
        super(deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix, recordSerializer);
        this.partitioner = partitioner;
        this.retry = retry;
        this.pendingRetryRecordsLimit = pendingRetryRecordsLimit;
        this.userDefinedProps = userDefinedProps;
    }

    /**
     * Create a {@link KafkaSinkBuilder} to construct a new {@link
     * org.apache.flink.connector.kafka.sink.KafkaSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link KafkaSinkBuilder}
     */
    public static <IN> RetryKafkaSinkBuilder<IN> builderRetryKafkaSink() {
        return new RetryKafkaSinkBuilder<>();
    }

    @Internal
    @Override
    public KafkaWriter<IN> createWriter(InitContext context) throws IOException {
        return new RetryKafkaWriter<IN>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                context.asSerializationSchemaInitializationContext(),
                Collections.emptyList(),
                partitioner,
                retry,
                pendingRetryRecordsLimit,
                ((SinkWriterOperator.InitContextImpl) context).getRuntimeContext(),
                userDefinedProps);
    }

    @Internal
    @Override
    public KafkaWriter<IN> restoreWriter(
            InitContext context, Collection<KafkaWriterState> recoveredState) throws IOException {
        return new RetryKafkaWriter<>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                context.asSerializationSchemaInitializationContext(),
                recoveredState,
                partitioner,
                retry,
                pendingRetryRecordsLimit,
                ((SinkWriterOperator.InitContextImpl) context).getRuntimeContext(),
                userDefinedProps);
    }
}
