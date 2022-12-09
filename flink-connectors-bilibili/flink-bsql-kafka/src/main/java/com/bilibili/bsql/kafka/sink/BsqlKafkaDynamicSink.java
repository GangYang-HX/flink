package com.bilibili.bsql.kafka.sink;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink;
import org.apache.flink.streaming.connectors.kafka.table.SinkBufferFlushMode;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import com.bilibili.bsql.kafka.partitioner.RandomRetryPartitioner;
import com.bilibili.bsql.kafka.schema.RetryDynamicKafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/** A version-agnostic Kafka {@link DynamicTableSink}. */
@Internal
public class BsqlKafkaDynamicSink extends KafkaDynamicSink {
    /** retry to send records when exception. */
    protected Boolean kafkaRetry;

    protected Integer pendingRetryRecordsLimit;
    protected Properties userDefinedProps;
    protected Properties blacklistProps;

    public BsqlKafkaDynamicSink(
            DataType consumedDataType,
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String topic,
            Properties properties,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            DeliveryGuarantee deliveryGuarantee,
            boolean upsertMode,
            SinkBufferFlushMode flushMode,
            @Nullable Integer parallelism,
            @Nullable String transactionalIdPrefix,
            Boolean kafkaRetry,
            Integer pendingRetryRecordsLimit,
            Properties userDefinedProps) {
        super(
                consumedDataType,
                physicalDataType,
                keyEncodingFormat,
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topic,
                properties,
                partitioner,
                deliveryGuarantee,
                upsertMode,
                flushMode,
                parallelism,
                transactionalIdPrefix);
        this.kafkaRetry = kafkaRetry;
        this.pendingRetryRecordsLimit = pendingRetryRecordsLimit;
        this.userDefinedProps = userDefinedProps;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> keySerialization =
                createSerialization(context, keyEncodingFormat, keyProjection, keyPrefix);

        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection, null);

        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        final RetryKafkaSinkBuilder<RowData> sinkBuilder = RetryKafkaSink.builderRetryKafkaSink();
        if (transactionalIdPrefix != null) {
            sinkBuilder.setTransactionalIdPrefix(transactionalIdPrefix);
        }

        if (partitioner instanceof RandomRetryPartitioner) {
            ((RandomRetryPartitioner) partitioner).setKafkaFailRetry(kafkaRetry);
        }

        final KafkaSink<RowData> kafkaSink =
                sinkBuilder
                        .setDeliverGuarantee(deliveryGuarantee)
                        .setBootstrapServers(
                                properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString())
                        .setKafkaProducerConfig(properties)
                        .setUserDefinedProps(userDefinedProps)
                        .setPartitoner(partitioner)
                        .setRetry(kafkaRetry)
                        .setPendingRetryRecordsLimit(pendingRetryRecordsLimit)
                        .setRecordSerializer(
                                new RetryDynamicKafkaRecordSerializationSchema(
                                        topic,
                                        partitioner,
                                        keySerialization,
                                        valueSerialization,
                                        getFieldGetters(physicalChildren, keyProjection),
                                        getFieldGetters(physicalChildren, valueProjection),
                                        hasMetadata(),
                                        getMetadataPositions(physicalChildren),
                                        upsertMode,
                                        userDefinedProps))
                        .build();
        return SinkV2Provider.of(kafkaSink, parallelism);

        // do not support upsert
    }

    @Override
    public DynamicTableSink copy() {
        final BsqlKafkaDynamicSink copy =
                new BsqlKafkaDynamicSink(
                        consumedDataType,
                        physicalDataType,
                        keyEncodingFormat,
                        valueEncodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        topic,
                        properties,
                        partitioner,
                        deliveryGuarantee,
                        upsertMode,
                        flushMode,
                        parallelism,
                        transactionalIdPrefix,
                        kafkaRetry,
                        pendingRetryRecordsLimit,
                        userDefinedProps);
        copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Bsql Kafka table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BsqlKafkaDynamicSink that = (BsqlKafkaDynamicSink) o;
        return Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyEncodingFormat, that.keyEncodingFormat)
                && Objects.equals(valueEncodingFormat, that.valueEncodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(topic, that.topic)
                && Objects.equals(properties, that.properties)
                && Objects.equals(partitioner, that.partitioner)
                && Objects.equals(deliveryGuarantee, that.deliveryGuarantee)
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(flushMode, that.flushMode)
                && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                && Objects.equals(parallelism, that.parallelism)
                && Objects.equals(kafkaRetry, that.kafkaRetry)
                && Objects.equals(pendingRetryRecordsLimit, that.pendingRetryRecordsLimit)
                && Objects.equals(userDefinedProps, that.userDefinedProps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                metadataKeys,
                consumedDataType,
                physicalDataType,
                keyEncodingFormat,
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topic,
                properties,
                partitioner,
                deliveryGuarantee,
                upsertMode,
                flushMode,
                transactionalIdPrefix,
                parallelism,
                kafkaRetry,
                pendingRetryRecordsLimit,
                userDefinedProps);
    }
}
