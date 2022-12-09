
package com.bilibili.bsql.kafka.diversion.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink;
import org.apache.flink.streaming.connectors.kafka.table.SinkBufferFlushMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.DataType;

import com.bilibili.bsql.kafka.BsqlKafkaDynamicTableFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.bilibili.bsql.kafka.diversion.tableinfo.BsqlKafkaDiversionConfig.BSQL_BROKER_UDF;
import static com.bilibili.bsql.kafka.diversion.tableinfo.BsqlKafkaDiversionConfig.BSQL_TOPIC_UDF;
import static com.bilibili.bsql.kafka.diversion.tableinfo.BsqlKafkaDiversionConfig.CACHE_TTL;
import static com.bilibili.bsql.kafka.diversion.tableinfo.BsqlKafkaDiversionConfig.EXCLUDE_ARG_FIELD;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.BSQL_PROPS_KAFKA_RETRY;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.BSQL_PROPS_PENDING_RETRY_RECORDS_LIMIT;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.SINK_CUSTOM_TRACE_CLASS;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.SINK_TRACE_ID;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.SINK_TRACE_KIND;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.autoCompleteSchemaRegistrySubject;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getKafkaProperties;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.validateTableSinkOptions;

/** BsqlKafkaDiversionDynamicSinkFactory. */
public class BsqlKafkaDiversionDynamicSinkFactory extends BsqlKafkaDynamicTableFactory {

    public static final String IDENTIFIER = "bsql-kafka-diversion";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> configOptions = super.requiredOptions();
        configOptions.add(BSQL_TOPIC_UDF);
        return configOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> configOptions = super.optionalOptions();
        configOptions.add(BSQL_BROKER_UDF);
        configOptions.add(CACHE_TTL);
        configOptions.add(EXCLUDE_ARG_FIELD);

        // trace option
        configOptions.add(SINK_TRACE_ID);
        configOptions.add(SINK_TRACE_KIND);
        configOptions.add(SINK_CUSTOM_TRACE_CLASS);
        return configOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        this, autoCompleteSchemaRegistrySubject(context));

        final ReadableConfig tableOptions = helper.getOptions();

        List<String> columnNames = context.getCatalogTable().getResolvedSchema().getColumnNames();

        final Optional<EncodingFormat<SerializationSchema<RowData>>> keyEncodingFormat =
                getKeyEncodingFormat(helper);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);

        helper.validateExcept(PROPERTIES_PREFIX);

        final DeliveryGuarantee deliveryGuarantee = validateDeprecatedSemantic(tableOptions);
        validateTableSinkOptions(tableOptions);

        KafkaConnectorOptionsUtil.validateDeliveryGuarantee(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                valueEncodingFormat);

        final DataType physicalDataType =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);

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
                tableOptions.get(BSQL_TOPIC_UDF),
                tableOptions.get(BSQL_BROKER_UDF),
                tableOptions.get(CACHE_TTL),
                tableOptions.get(EXCLUDE_ARG_FIELD),
                tableOptions.get(PROPS_BOOTSTRAP_SERVERS),
                columnNames,
                kafkaRetry,
                pendingRetryRecordsLimit,
                userDefinedProps);
    }

    private KafkaDynamicSink createKafkaTableSink(
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
            String topicUdfClassName,
            String brokerUdfClassName,
            String cacheTtl,
            Boolean excludeField,
            String broker,
            List<String> fieldNames,
            Boolean kafkaRetry,
            Integer pendingRetryRecordsLimit,
            Properties userDefinedProps) {
        return new BsqlKafkaDiversionDynamicSink(
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
                topicUdfClassName,
                brokerUdfClassName,
                Integer.parseInt(cacheTtl),
                excludeField,
                broker,
                fieldNames,
                kafkaRetry,
                pendingRetryRecordsLimit,
                userDefinedProps);
    }
}
