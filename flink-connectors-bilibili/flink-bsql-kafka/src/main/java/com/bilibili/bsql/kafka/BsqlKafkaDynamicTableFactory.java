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

import com.bilibili.bsql.kafka.sink.BsqlKafkaDynamicSink;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.streaming.connectors.kafka.table.SinkBufferFlushMode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.BSQL_PROPS_KAFKA_RETRY;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.BSQL_PROPS_PENDING_RETRY_RECORDS_LIMIT;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.PARTITION_STRATEGY;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.SINK_CUSTOM_TRACE_CLASS;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.SINK_TRACE_ID;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.SINK_TRACE_KIND;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.autoCompleteSchemaRegistrySubject;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getKafkaProperties;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.validateTableSinkOptions;

/**
 * Factory for creating configured instances of {@link KafkaDynamicSource} and {@link
 * KafkaDynamicSink}.
 */
@Internal
public class BsqlKafkaDynamicTableFactory extends KafkaDynamicTableFactory {
    private static final Logger LOG = LoggerFactory.getLogger(BsqlKafkaDynamicTableFactory.class);

    public static final String IDENTIFIER = "bsql-kafka";

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

        // trace option
        options.add(SINK_TRACE_ID);
        options.add(SINK_TRACE_KIND);
        options.add(SINK_CUSTOM_TRACE_CLASS);
        return options;
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

        userDefinedProps.put(SINK_TRACE_ID.key(), tableOptions.get(SINK_TRACE_ID));
        userDefinedProps.put(SINK_TRACE_KIND.key(), tableOptions.get(SINK_TRACE_KIND));
        userDefinedProps.put(
                SINK_CUSTOM_TRACE_CLASS.key(), tableOptions.get(SINK_CUSTOM_TRACE_CLASS));

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
}
