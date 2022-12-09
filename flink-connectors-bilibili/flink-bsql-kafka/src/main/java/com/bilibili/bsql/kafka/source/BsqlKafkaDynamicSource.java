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

package com.bilibili.bsql.kafka.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CollectionUtil;

import com.bilibili.bsql.kafka.schema.LancerKafkaDeserializationSchema;
import com.bilibili.bsql.kafka.util.KafkaTPDetector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.USE_LANCER_DESERIALIZATION_SCHEMA;

/** A version-agnostic Kafka {@link org.apache.flink.table.connector.source.ScanTableSource}. */
@Internal
public class BsqlKafkaDynamicSource extends KafkaDynamicSource {

    protected Properties userDefinedProps;

    private Integer minNumTopicPartitions = -1;

    public BsqlKafkaDynamicSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> topics,
            @Nullable Pattern topicPattern,
            Properties properties,
            StartupMode startupMode,
            Map<KafkaTopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis,
            boolean upsertMode,
            String tableIdentifier,
            Properties userDefinedProps) {
        super(
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                upsertMode,
                tableIdentifier);
        this.userDefinedProps = userDefinedProps;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(context, keyDecodingFormat, keyProjection, keyPrefix);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection, null);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final KafkaSource<RowData> kafkaSource =
                createKafkaSource(keyDeserialization, valueDeserialization, producedTypeInfo);
        if (!CollectionUtil.isNullOrEmpty(topics)) {
            minNumTopicPartitions =
                    KafkaTPDetector.getTopicTpNumber(properties, String.join(",", topics));
        }
        return new BsqlKafkaDataStreamScanProvider(
                watermarkStrategy,
                minNumTopicPartitions,
                kafkaSource,
                tableIdentifier,
                KAFKA_TRANSFORMATION);
    }

    protected KafkaSource<RowData> createKafkaSource(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final KafkaDeserializationSchema<RowData> kafkaDeserializer =
                createKafkaDeserializationSchema(
                        keyDeserialization, valueDeserialization, producedTypeInfo);

        final KafkaSourceBuilder<RowData> kafkaSourceBuilder = KafkaSource.builder();

        if (topics != null) {
            kafkaSourceBuilder.setTopics(topics);
        } else {
            kafkaSourceBuilder.setTopicPattern(topicPattern);
        }

        switch (startupMode) {
            case EARLIEST:
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
                break;
            case LATEST:
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                String offsetResetConfig =
                        properties.getProperty(
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                OffsetResetStrategy.NONE.name());
                OffsetResetStrategy offsetResetStrategy = getResetStrategy(offsetResetConfig);
                kafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.committedOffsets(offsetResetStrategy));
                break;
            case SPECIFIC_OFFSETS:
                Map<TopicPartition, Long> offsets = new HashMap<>();
                specificStartupOffsets.forEach(
                        (tp, offset) ->
                                offsets.put(
                                        new TopicPartition(tp.getTopic(), tp.getPartition()),
                                        offset));
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.offsets(offsets));
                break;
            case TIMESTAMP:
                kafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.timestamp(startupTimestampMillis));
                break;
        }

        kafkaSourceBuilder
                .setProperties(properties)
                .setProperties(userDefinedProps)
                .setDeserializer(KafkaRecordDeserializationSchema.of(kafkaDeserializer));

        return kafkaSourceBuilder.build();
    }

    protected KafkaDeserializationSchema<RowData> createKafkaDeserializationSchema(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {
        final DynamicKafkaDeserializationSchema.MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(DynamicKafkaDeserializationSchema.MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                DataType.getFieldDataTypes(producedDataType).size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                                IntStream.of(valueProjection),
                                IntStream.range(
                                        keyProjection.length + valueProjection.length,
                                        adjustedPhysicalArity))
                        .toArray();
        return (Boolean) userDefinedProps.get(USE_LANCER_DESERIALIZATION_SCHEMA.key())
                ? new LancerKafkaDeserializationSchema(
                        adjustedPhysicalArity,
                        keyDeserialization,
                        keyProjection,
                        valueDeserialization,
                        adjustedValueProjection,
                        hasMetadata,
                        metadataConverters,
                        producedTypeInfo,
                        upsertMode,
                        userDefinedProps)
                : new DynamicKafkaDeserializationSchema(
                        adjustedPhysicalArity,
                        keyDeserialization,
                        keyProjection,
                        valueDeserialization,
                        adjustedValueProjection,
                        hasMetadata,
                        metadataConverters,
                        producedTypeInfo,
                        upsertMode);
    }

    @Override
    public DynamicTableSource copy() {
        final BsqlKafkaDynamicSource copy =
                new BsqlKafkaDynamicSource(
                        physicalDataType,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        topics,
                        topicPattern,
                        properties,
                        startupMode,
                        specificStartupOffsets,
                        startupTimestampMillis,
                        upsertMode,
                        tableIdentifier,
                        userDefinedProps);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "bsql-kafka table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BsqlKafkaDynamicSource that = (BsqlKafkaDynamicSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyDecodingFormat, that.keyDecodingFormat)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(topics, that.topics)
                && Objects.equals(String.valueOf(topicPattern), String.valueOf(that.topicPattern))
                && Objects.equals(properties, that.properties)
                && startupMode == that.startupMode
                && Objects.equals(specificStartupOffsets, that.specificStartupOffsets)
                && startupTimestampMillis == that.startupTimestampMillis
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(tableIdentifier, that.tableIdentifier)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy)
                && Objects.equals(userDefinedProps, that.userDefinedProps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                metadataKeys,
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                upsertMode,
                tableIdentifier,
                watermarkStrategy,
                userDefinedProps);
    }
}
