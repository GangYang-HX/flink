package com.bilibili.bsql.kafka.diversion.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaWriter;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.operators.sink.InitContextInitializationContextAdapter;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator;

import com.bilibili.bsql.kafka.sink.RetryKafkaSink;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * BsqlDiversionKafkaSink.
 *
 * @param <IN>
 */
public class BsqlDiversionKafkaSink<IN> extends RetryKafkaSink<IN> {

    private Properties userDefinedProps;

    public BsqlDiversionKafkaSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            FlinkKafkaPartitioner<IN> partitioner,
            Boolean retry,
            Integer pendingRetryRecordsLimit,
            Properties userDefinedProps) {
        super(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                recordSerializer,
                partitioner,
                retry,
                pendingRetryRecordsLimit,
                userDefinedProps);
        this.userDefinedProps = userDefinedProps;
    }

    public static <IN> DiversionKafkaSinkBuilder<IN> builderDiversion() {
        return new DiversionKafkaSinkBuilder<>();
    }

    @Override
    public KafkaWriter<IN> createWriter(InitContext context) throws IOException {
        final Supplier<MetricGroup> metricGroupSupplier =
                () -> context.metricGroup().addGroup("user");
        kafkaProducerConfig.put("isDiversion", "true");
        return new DiversionKafkaWriter<IN>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                new InitContextInitializationContextAdapter(
                        context.getUserCodeClassLoader(), metricGroupSupplier),
                Collections.emptyList(),
                partitioner,
                retry,
                pendingRetryRecordsLimit,
                ((SinkWriterOperator.InitContextImpl) context).getRuntimeContext(),
                this.userDefinedProps);
    }

    @Override
    public KafkaWriter<IN> restoreWriter(
            InitContext context, Collection<KafkaWriterState> recoveredState) throws IOException {
        final Supplier<MetricGroup> metricGroupSupplier =
                () -> context.metricGroup().addGroup("user");
        kafkaProducerConfig.put("isDiversion", "true");
        return new DiversionKafkaWriter<IN>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                new InitContextInitializationContextAdapter(
                        context.getUserCodeClassLoader(), metricGroupSupplier),
                recoveredState,
                partitioner,
                retry,
                pendingRetryRecordsLimit,
                ((SinkWriterOperator.InitContextImpl) context).getRuntimeContext(),
                this.userDefinedProps);
    }
}
