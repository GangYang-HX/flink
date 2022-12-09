
package com.bilibili.bsql.kafka.diversion.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.SinkBufferFlushMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import com.bilibili.bsql.kafka.diversion.schema.DiversionDynamicKafkaRecordSerializationSchema;
import com.bilibili.bsql.kafka.diversion.selector.CacheBrokerSelector;
import com.bilibili.bsql.kafka.diversion.selector.CacheTopicSelector;
import com.bilibili.bsql.kafka.diversion.tableinfo.BsqlKafkaDiversionConfig;
import com.bilibili.bsql.kafka.sink.BsqlKafkaDynamicSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

/** BsqlKafkaDiversionDynamicSink. */
public class BsqlKafkaDiversionDynamicSink extends BsqlKafkaDynamicSink {

    private static final Logger LOG = LoggerFactory.getLogger(BsqlKafkaDiversionDynamicSink.class);

    private final String topicUdfClassName;
    private final String broker;
    private final String brokerUdfClassName;
    private final List<String> fieldNames;
    private final int cacheTtl;
    private final boolean excludeField;

    private final Properties userDefinedProps;

    public BsqlKafkaDiversionDynamicSink(
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
            String topicUdfClassName,
            String brokerUdfClassName,
            int cacheTtl,
            Boolean excludeField,
            String broker,
            List<String> fieldNames,
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
                transactionalIdPrefix,
                kafkaRetry,
                pendingRetryRecordsLimit,
                userDefinedProps);
        this.topicUdfClassName = topicUdfClassName;
        this.brokerUdfClassName = brokerUdfClassName;
        this.cacheTtl = cacheTtl;
        this.excludeField = excludeField;
        this.broker = broker;
        this.fieldNames = fieldNames;
        this.userDefinedProps = userDefinedProps;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> keySerialization =
                createSerialization(context, keyEncodingFormat, keyProjection, keyPrefix);

        DataStructureConverter converter = context.createDataStructureConverter(consumedDataType);
        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection, null);

        final DiversionKafkaSinkBuilder<RowData> sinkBuilder =
                BsqlDiversionKafkaSink.builderDiversion();
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();
        if (transactionalIdPrefix != null) {
            sinkBuilder.setTransactionalIdPrefix(transactionalIdPrefix);
        }
        // valid udf class
        Class<?> topicUdfClass = getAndVaildUdf(topicUdfClassName, topic);
        Class<?> brokerUdfClass = getAndVaildUdf(brokerUdfClassName, broker);
        if (topicUdfClass == null && brokerUdfClass == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "You need to specify the udf class full name with '%s' or " + "'%s'",
                            BsqlKafkaDiversionConfig.BSQL_TOPIC_UDF.key(),
                            BsqlKafkaDiversionConfig.BSQL_BROKER_UDF.key()));
        }
        int[] topicFieldIndex = new int[0];
        int[] brokerFieldIndex = new int[0];
        // valid method
        if (topicUdfClass != null) {
            validMethod(topicUdfClass, topic);
            // find columns index
            topicFieldIndex = findFieldIndex(topic);
        }
        if (brokerUdfClass != null) {
            validMethod(brokerUdfClass, broker);
            // find columns index
            brokerFieldIndex = findFieldIndex(broker);
        }
        TopicSelector<RowData> topicSelector =
                new CacheTopicSelector(cacheTtl, topicUdfClass, converter, topicFieldIndex);
        TopicSelector<RowData> brokerSelector =
                new CacheBrokerSelector(cacheTtl, brokerUdfClass, converter, brokerFieldIndex);
        final BsqlDiversionKafkaSink<RowData> kafkaSink =
                sinkBuilder
                        .setDeliverGuarantee(deliveryGuarantee)
                        .setBootstrapServers(
                                properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString())
                        .setKafkaProducerConfig(properties)
                        .setPartitoner(partitioner)
                        .setRetry(kafkaRetry)
                        .setPendingRetryRecordsLimit(pendingRetryRecordsLimit)
                        .setRecordSerializer(
                                new DiversionDynamicKafkaRecordSerializationSchema(
                                        topic,
                                        partitioner,
                                        keySerialization,
                                        valueSerialization,
                                        getFieldGetters(physicalChildren, keyProjection),
                                        getFieldGetters(physicalChildren, valueProjection),
                                        hasMetadata(),
                                        getMetadataPositions(physicalChildren),
                                        upsertMode,
                                        topicSelector,
                                        brokerSelector,
                                        userDefinedProps))
                        .build();
        // do not support upsert
        return SinkV2Provider.of(kafkaSink, parallelism);
    }

    private Class<?> getAndVaildUdf(String udfClassName, String udfProperty) {
        String propertyClassName = udfProperty.split("\\(")[0];
        if (StringUtils.isNullOrWhitespaceOnly(udfClassName)) {
            return null;
        } else {
            if (propertyClassName.equals(
                    udfClassName.substring(udfClassName.lastIndexOf(".") + 1))) {
                try {
                    return Class.forName(udfClassName);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException(
                            "can not find udf class named '" + udfClassName + "'");
                }
            } else {
                throw new IllegalArgumentException(
                        "You need to specify the udf class full name for " + propertyClassName);
            }
        }
    }

    private void validMethod(Class<?> udfClass, String udfProperty) {
        int evalMethodCount = 0;
        Method evalMethod = null;
        for (Method method : udfClass.getMethods()) {
            if ("eval".equals(method.getName())) {
                evalMethod = method;
                evalMethodCount++;
            }
        }
        if (evalMethodCount != 1) {
            throw new IllegalArgumentException(
                    "the " + udfClass + " class don't have unique eval method");
        }
        if (evalMethod.getParameterCount()
                != udfProperty.split("\\(")[1].replaceAll("\\)", "").trim().split(",").length) {
            throw new IllegalArgumentException(
                    udfClass.getSimpleName() + ": The number of parameters does not match.");
        }
    }

    private int[] findFieldIndex(String udfProperty) {
        int[] filedIndex;
        String[] inputColumns = udfProperty.split("\\(")[1].replaceAll("\\)", "").trim().split(",");
        filedIndex = new int[inputColumns.length];
        List<String> fieldNames = ((RowType) consumedDataType.getLogicalType()).getFieldNames();
        for (int i = 0; i < inputColumns.length; i++) {
            for (int j = 0; j < fieldNames.size(); j++) {
                if (inputColumns[i].trim().equals(fieldNames.get(j))) {
                    filedIndex[i] = j;
                    LOG.info("input column:{}, field index:{}", inputColumns[i].trim(), j);
                }
            }
        }
        return filedIndex;
    }
}
