package com.bilibili.bsql.kafka.diversion.schema;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.bilibili.bsql.kafka.schema.RetryDynamicKafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Properties;
import java.util.function.Function;

/** DiversionDynamicKafkaRecordSerializationSchema. */
public class DiversionDynamicKafkaRecordSerializationSchema
        extends RetryDynamicKafkaRecordSerializationSchema {

    private static final Logger LOG =
            LoggerFactory.getLogger(DiversionDynamicKafkaRecordSerializationSchema.class);

    private final Function<? super RowData, String> topicSelector;

    private final Function<? super RowData, String> brokerSelector;

    public DiversionDynamicKafkaRecordSerializationSchema(
            String topic,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            @Nullable SerializationSchema<RowData> keySerialization,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions,
            boolean upsertMode,
            Function<? super RowData, String> topicSelector,
            Function<? super RowData, String> brokerSelector,
            Properties userDefinedProps) {
        super(
                topic,
                partitioner,
                keySerialization,
                valueSerialization,
                keyFieldGetters,
                valueFieldGetters,
                hasMetadata,
                metadataPositions,
                upsertMode,
                userDefinedProps);
        this.topicSelector = topicSelector;
        this.brokerSelector = brokerSelector;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        // init traceClient
        super.open(context, sinkContext);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RowData rowData, KafkaSinkContext context, Long timestamp) {
        try {
            String topic = topicSelector.apply(rowData);
            // shortcut in case no input projection is required
            if (keySerialization == null && !hasMetadata) {
                final byte[] valueSerialized = valueSerialization.serialize(rowData);
                return new ProducerRecord<>(
                        topic,
                        extractPartition(
                                topic,
                                rowData,
                                null,
                                valueSerialized,
                                context.getPartitionsForTopic(
                                        topic, brokerSelector.apply(rowData))),
                        null,
                        valueSerialized);
            }
            final byte[] keySerialized;
            if (keySerialization == null) {
                keySerialized = null;
            } else {
                final RowData keyRow = createProjectedRow(rowData, RowKind.INSERT, keyFieldGetters);
                keySerialized = keySerialization.serialize(keyRow);
            }

            final byte[] valueSerialized;
            final RowKind kind = rowData.getRowKind();
            if (upsertMode) {
                if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                    // transform the message as the tombstone message
                    valueSerialized = null;
                } else {
                    // make the message to be INSERT to be compliant with the INSERT-ONLY format
                    final RowData valueRow =
                            DynamicKafkaRecordSerializationSchema.createProjectedRow(
                                    rowData, kind, valueFieldGetters);
                    valueRow.setRowKind(RowKind.INSERT);
                    valueSerialized = valueSerialization.serialize(valueRow);
                }
            } else {
                final RowData valueRow =
                        DynamicKafkaRecordSerializationSchema.createProjectedRow(
                                rowData, kind, valueFieldGetters);
                valueSerialized = valueSerialization.serialize(valueRow);
            }

            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(
                            topic,
                            extractPartition(
                                    topic,
                                    rowData,
                                    keySerialized,
                                    valueSerialized,
                                    context.getPartitionsForTopic(
                                            topic, brokerSelector.apply(rowData))),
                            readMetadata(rowData, KafkaDynamicSink.WritableMetadata.TIMESTAMP),
                            keySerialized,
                            valueSerialized,
                            readMetadata(rowData, KafkaDynamicSink.WritableMetadata.HEADERS));
            super.traceEvent(record, this.traceId, 1, "", "lancer2-bsql", "", false, "");
            return record;
        } catch (Exception e) {
            super.traceEvent(
                    null, this.traceId, 1, "", "lancer2-bsql", "", true, "serialize error");
            throw e;
        }
    }

    @Override
    public String getBrokers(RowData element) {
        return brokerSelector.apply(element);
    }
}
