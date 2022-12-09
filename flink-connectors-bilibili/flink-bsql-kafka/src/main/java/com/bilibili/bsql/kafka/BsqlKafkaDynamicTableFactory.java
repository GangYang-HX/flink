package com.bilibili.bsql.kafka;

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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.streaming.connectors.kafka.table.SinkBufferFlushMode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;

import com.bilibili.bsql.kafka.sink.BsqlKafkaDynamicSink;
import com.bilibili.bsql.kafka.source.BsqlKafkaDynamicSource;
import com.bilibili.bsql.kafka.util.ParseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.BSQL_DEFAULT_OFFSET_RESET;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.BSQL_OFFSET_RESET;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.BSQL_PROPS_KAFKA_RETRY;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.BSQL_PROPS_PENDING_RETRY_RECORDS_LIMIT;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.CUSTOM_TRACE_CLASS;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.PARTITION_STRATEGY;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.SINK_DEST;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.TRACE_ID;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.TRACE_KIND;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.USE_LANCER_DESERIALIZATION_SCHEMA;
import static org.apache.flink.connector.kafka.source.KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.kafka.source.KafkaSourceOptions.PARTITION_DISCOVERY_RETRIES;
import static org.apache.flink.connector.kafka.source.KafkaSourceOptions.PARTITION_DISCOVERY_TIMEOUT_MS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_ENABLE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_KICK_THRESHOLD;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_LAG_THRESHOLD;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_LAG_TIMES;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_ZK_HOST;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_ZK_ROOT_PATH;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.autoCompleteSchemaRegistrySubject;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getKafkaProperties;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getSourceTopicPattern;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getSourceTopics;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getStartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.validateTableSinkOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.validateTableSourceOptions;
import static org.apache.flink.table.factories.FactoryUtil.BSQL_SABER_JOB_ID;

/**
 * Factory for creating configured instances of {@link KafkaDynamicSource} and {@link
 * KafkaDynamicSink}.
 */
@Internal
public class BsqlKafkaDynamicTableFactory extends KafkaDynamicTableFactory {
    private static final Logger LOG = LoggerFactory.getLogger(BsqlKafkaDynamicTableFactory.class);

    public static final String IDENTIFIER = "bsql-kafka";

    public static final String OFFSET_RESET_EARLIEST = "earliest";
    public static final String OFFSET_RESET_LATEST = "latest";
    public static final String KAFKA_OFFSET_RESET_KEY = "properties.auto.offset.reset";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return super.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = super.optionalOptions();
        options.add(BSQL_PROPS_KAFKA_RETRY);
        options.add(PARTITION_STRATEGY);
        options.add(BSQL_PROPS_PENDING_RETRY_RECORDS_LIMIT);
        options.add(PARTITION_DISCOVERY_INTERVAL_MS);
        options.add(PARTITION_DISCOVERY_TIMEOUT_MS);
        options.add(PARTITION_DISCOVERY_RETRIES);

        // trace option
        options.add(TRACE_ID);
        options.add(TRACE_KIND);
        options.add(CUSTOM_TRACE_CLASS);
        options.add(SINK_DEST);
        // blacklist

        options.add(BLACKLIST_ENABLE);
        options.add(BLACKLIST_KICK_THRESHOLD);
        options.add(BLACKLIST_LAG_THRESHOLD);
        options.add(BLACKLIST_LAG_TIMES);
        options.add(BLACKLIST_ZK_HOST);
        options.add(BLACKLIST_ZK_ROOT_PATH);
        // lancer deserialization schama
        options.add(USE_LANCER_DESERIALIZATION_SCHEMA);
        // offset policy
        options.add(BSQL_OFFSET_RESET);
        options.add(BSQL_DEFAULT_OFFSET_RESET);

        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        autoFillingBrokerInfo(context);
        transformStartupOptions(context);
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        // transform bsql kafka info to original
        transformKafkaProperties(tableOptions, context);

        final KafkaConnectorOptionsUtil.StartupOptions startupOptions =
                getStartupOptions(tableOptions);
        validateTableSourceOptions(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                valueDecodingFormat);

        final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());

        // add topic-partition discovery
        properties.setProperty(
                PARTITION_DISCOVERY_INTERVAL_MS.key(),
                tableOptions.get(PARTITION_DISCOVERY_INTERVAL_MS).toString());
        // discovery topic-partition timeout
        properties.setProperty(
                PARTITION_DISCOVERY_TIMEOUT_MS.key(),
                tableOptions.get(PARTITION_DISCOVERY_TIMEOUT_MS).toString());
        // times to retry when discovery partition encounter exception
        properties.setProperty(
                PARTITION_DISCOVERY_RETRIES.key(),
                tableOptions.get(PARTITION_DISCOVERY_RETRIES).toString());

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        Properties userDefinedProps = new Properties();
        userDefinedProps.put(BLACKLIST_ENABLE.key(), tableOptions.get(BLACKLIST_ENABLE));
        userDefinedProps.put(
                BLACKLIST_KICK_THRESHOLD.key(), tableOptions.get(BLACKLIST_KICK_THRESHOLD));
        userDefinedProps.put(
                BLACKLIST_LAG_THRESHOLD.key(), tableOptions.get(BLACKLIST_LAG_THRESHOLD));
        userDefinedProps.put(BLACKLIST_LAG_TIMES.key(), tableOptions.get(BLACKLIST_LAG_TIMES));
        userDefinedProps.put(BLACKLIST_ZK_HOST.key(), tableOptions.get(BLACKLIST_ZK_HOST));
        userDefinedProps.put(
                BLACKLIST_ZK_ROOT_PATH.key(), tableOptions.get(BLACKLIST_ZK_ROOT_PATH));

        userDefinedProps.put(
                USE_LANCER_DESERIALIZATION_SCHEMA.key(),
                tableOptions.get(USE_LANCER_DESERIALIZATION_SCHEMA));

        // trace
        userDefinedProps.put(TRACE_ID.key(), tableOptions.get(TRACE_ID));
        userDefinedProps.put(SINK_DEST.key(), tableOptions.get(SINK_DEST));
        userDefinedProps.put(TRACE_KIND.key(), tableOptions.get(TRACE_KIND));
        userDefinedProps.put(CUSTOM_TRACE_CLASS.key(), tableOptions.get(CUSTOM_TRACE_CLASS));

        return createKafkaTableSource(
                physicalDataType,
                keyDecodingFormat.orElse(null),
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                getSourceTopics(tableOptions),
                getSourceTopicPattern(tableOptions),
                properties,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.startupTimestampMillis,
                context.getObjectIdentifier().asSummaryString(),
                userDefinedProps);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        autoFillingBrokerInfo(context);
        final TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        this, autoCompleteSchemaRegistrySubject(context));

        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                getKeyEncodingFormat(helper);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();
        final DeliveryGuarantee deliveryGuarantee = validateDeprecatedSemantic(tableOptions);
        validateTableSinkOptions(tableOptions);

        KafkaConnectorOptionsUtil.validateDeliveryGuarantee(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                valueEncodingFormat);

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);

        // TODO hash partitioner conflict with kafka retry
        final Boolean kafkaRetry = tableOptions.get(BSQL_PROPS_KAFKA_RETRY);

        final Integer pendingRetryRecordsLimit =
                tableOptions.get(BSQL_PROPS_PENDING_RETRY_RECORDS_LIMIT);

        Properties userDefinedProps = new Properties();

        userDefinedProps.put(TRACE_ID.key(), tableOptions.get(TRACE_ID));
        userDefinedProps.put(TRACE_KIND.key(), tableOptions.get(TRACE_KIND));
        userDefinedProps.put(CUSTOM_TRACE_CLASS.key(), tableOptions.get(CUSTOM_TRACE_CLASS));

        userDefinedProps.put(BLACKLIST_ENABLE.key(), tableOptions.get(BLACKLIST_ENABLE));
        userDefinedProps.put(
                BLACKLIST_KICK_THRESHOLD.key(), tableOptions.get(BLACKLIST_KICK_THRESHOLD));
        userDefinedProps.put(BLACKLIST_ZK_HOST.key(), tableOptions.get(BLACKLIST_ZK_HOST));
        userDefinedProps.put(
                BLACKLIST_ZK_ROOT_PATH.key(), tableOptions.get(BLACKLIST_ZK_ROOT_PATH));

        userDefinedProps.put(BLACKLIST_ENABLE.key(), tableOptions.get(BLACKLIST_ENABLE));
        userDefinedProps.put(
                BLACKLIST_KICK_THRESHOLD.key(), tableOptions.get(BLACKLIST_KICK_THRESHOLD));
        userDefinedProps.put(BLACKLIST_ZK_HOST.key(), tableOptions.get(BLACKLIST_ZK_HOST));
        userDefinedProps.put(
                BLACKLIST_ZK_ROOT_PATH.key(), tableOptions.get(BLACKLIST_ZK_ROOT_PATH));

        return createKafkaTableSink(
                physicalDataType,
                keyEncodingFormat.orElse(null),
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                tableOptions.get(TOPIC).get(0),
                getKafkaProperties(context.getCatalogTable().getOptions()),
                getFlinkKafkaPartitioner(tableOptions, context.getClassLoader()),
                deliveryGuarantee,
                parallelism,
                tableOptions.get(TRANSACTIONAL_ID_PREFIX),
                kafkaRetry,
                pendingRetryRecordsLimit,
                userDefinedProps);
    }

    // --------------------------------------------------------------------------------------------

    protected FlinkKafkaPartitioner<RowData> getFlinkKafkaPartitioner(
            ReadableConfig tableOptions, ClassLoader classLoader) {
        String partitionStrategy = tableOptions.get(PARTITION_STRATEGY);
        List<FlinkKafkaPartitioner<RowData>> foundPartitioner = new LinkedList<>();
        ServiceLoader.load(FlinkKafkaPartitioner.class, classLoader)
                .iterator()
                .forEachRemaining(foundPartitioner::add);
        List<FlinkKafkaPartitioner<RowData>> matchingPartitioner =
                foundPartitioner.stream()
                        .filter(p -> Objects.equals(p.partitionerIdentifier(), partitionStrategy))
                        .collect(Collectors.toList());
        if (matchingPartitioner.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any partitioner for partitionStrategy '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available partitioner partitionStrategy are:\n\n"
                                    + "%s",
                            partitionStrategy,
                            FlinkKafkaPartitioner.class.getName(),
                            foundPartitioner.stream()
                                    .map(FlinkKafkaPartitioner::partitionerIdentifier)
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingPartitioner.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple partitioner for partitionStrategy '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous partitioner classes are:\n\n"
                                    + "%s",
                            partitionStrategy,
                            FlinkKafkaPartitioner.class.getName(),
                            foundPartitioner.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        return matchingPartitioner.get(0);
    }

    protected KafkaDynamicSource createKafkaTableSource(
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
            String tableIdentifier,
            Properties userDefinedProps) {
        return new BsqlKafkaDynamicSource(
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
                false,
                tableIdentifier,
                userDefinedProps);
    }

    protected KafkaDynamicSink createKafkaTableSink(
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String topic,
            Properties properties,
            FlinkKafkaPartitioner<RowData> partitioner,
            DeliveryGuarantee deliveryGuarantee,
            Integer parallelism,
            @Nullable String transactionalIdPrefix,
            Boolean kafkaRetry,
            Integer pendingRetryRecordsLimit,
            Properties userDefinedProps) {
        return new BsqlKafkaDynamicSink(
                physicalDataType,
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
                false,
                SinkBufferFlushMode.DISABLED,
                parallelism,
                transactionalIdPrefix,
                kafkaRetry,
                pendingRetryRecordsLimit,
                userDefinedProps);
    }

    protected void transformKafkaProperties(ReadableConfig tableOptions, Context context) {
        String groupId = tableOptions.get(KafkaConnectorOptions.PROPS_GROUP_ID);
        Map<String, String> options = context.getCatalogTable().getOptions();
        String saberJobId = options.get(BSQL_SABER_JOB_ID.key());
        if (StringUtils.isNullOrWhitespaceOnly(groupId)
                && !StringUtils.isNullOrWhitespaceOnly(saberJobId)) {
            options.put(KafkaConnectorOptions.PROPS_GROUP_ID.key(), "saber_".concat(saberJobId));
        }
    }

    protected void transformStartupOptions(Context context) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        String offsetReset = options.get(BSQL_OFFSET_RESET.key());
        if (StringUtils.isNullOrWhitespaceOnly(offsetReset)) {
            return;
        }
        String defOffsetReset = options.get(BSQL_DEFAULT_OFFSET_RESET.key());
        boolean useTimeStamp = ParseUtils.isTimeStamp(offsetReset);
        if (useTimeStamp) {
            if (StringUtils.isNullOrWhitespaceOnly(defOffsetReset)) {
                throw new ValidationException(
                        "when offsetReset is a 13-bit timestamp,you should set the defOffsetReset policy: latest or earliest.");
            }
            options.put(
                    KafkaConnectorOptions.SCAN_STARTUP_MODE.key(), StartupMode.TIMESTAMP.name());
            options.put(KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key(), offsetReset);
            options.put(KAFKA_OFFSET_RESET_KEY, defOffsetReset);
        } else if (offsetReset.equals(OFFSET_RESET_EARLIEST)) {
            options.put(
                    KafkaConnectorOptions.SCAN_STARTUP_MODE.key(),
                    KafkaConnectorOptions.ScanStartupMode.EARLIEST_OFFSET.toString());
            options.put(KAFKA_OFFSET_RESET_KEY, offsetReset);
        } else if (offsetReset.equals(OFFSET_RESET_LATEST)) {
            options.put(
                    KafkaConnectorOptions.SCAN_STARTUP_MODE.key(),
                    KafkaConnectorOptions.ScanStartupMode.LATEST_OFFSET.toString());
            options.put(KAFKA_OFFSET_RESET_KEY, offsetReset);
        } else {
            throw new ValidationException(
                    "The offsetRest property must be a 13-bit timestamp or latest or earliest.");
        }
    }
}
