package com.bilibili.bsql.kafka.schema;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaRecordSerializationSchema;
import org.apache.flink.table.data.RowData;

import com.bilibili.bsql.trace.LogTracer;
import com.bilibili.bsql.trace.Trace;
import com.bilibili.bsql.trace.TraceUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.CUSTOM_TRACE_CLASS;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.TRACE_ID;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.TRACE_KIND;

/** SerializationSchema for Kafka sink. */
public class RetryDynamicKafkaRecordSerializationSchema
        extends DynamicKafkaRecordSerializationSchema implements LogTracer {

    private static final Logger LOG =
            LoggerFactory.getLogger(RetryDynamicKafkaRecordSerializationSchema.class);

    protected Trace traceClient;
    protected String traceId;
    protected String traceKind;
    protected String traceClass;

    protected Properties userDefinedProps;

    public RetryDynamicKafkaRecordSerializationSchema(
            String topic,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            @Nullable SerializationSchema<RowData> keySerialization,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions,
            boolean upsertMode,
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
                upsertMode);
        this.userDefinedProps = userDefinedProps;
        this.traceId = this.userDefinedProps.getOrDefault(TRACE_ID.key(), "").toString();
        this.traceKind =
                this.userDefinedProps
                        .getOrDefault(TRACE_KIND.key(), TRACE_KIND.defaultValue())
                        .toString();
        this.traceClass =
                this.userDefinedProps
                        .getOrDefault(CUSTOM_TRACE_CLASS.key(), CUSTOM_TRACE_CLASS.defaultValue())
                        .toString();
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        super.open(context, sinkContext);
        traceClient =
                Trace.createTraceClient(
                        Thread.currentThread().getContextClassLoader(),
                        this.traceKind,
                        this.traceClass,
                        this.traceId);
        LOG.info(
                "open RetryDynamicKafkaRecordSerializationSchema, userDefinedProps = {}, traceClient = {}",
                this.userDefinedProps,
                this.traceClient);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RowData consumedRow, KafkaSinkContext context, Long timestamp) {
        try {
            ProducerRecord<byte[], byte[]> record =
                    super.serialize(consumedRow, context, timestamp);
            this.traceEvent(record, this.traceId, 1, "", "lancer2-bsql", "", false, "");
            return record;
        } catch (Exception e) {
            this.traceEvent(null, this.traceId, 1, "", "lancer2-bsql", "", true, "serialize error");
            throw e;
        }
    }

    protected void traceEvent(
            ProducerRecord record,
            String traceId,
            long value,
            String url,
            String pipeline,
            String color,
            boolean isError,
            String errorMessage) {
        // parse record header
        Map<String, String> kafkaHeader =
                record == null
                        ? Collections.emptyMap()
                        : TraceUtils.parseKafkaHeader(record.headers());
        // parse record type
        int recordType = TraceUtils.parseRecordType(kafkaHeader, traceId);
        // parse traceId
        String parsedTraceId = TraceUtils.parseTraceId(kafkaHeader, recordType, traceId);
        // parse timestamp
        long parsedTimestamp = TraceUtils.parseTimestamp(kafkaHeader, recordType, record);
        // parse pipeline
        String parsedPipeline = TraceUtils.parsePipeline(recordType, pipeline);
        // parse color
        String parsedColor = TraceUtils.parseColor(kafkaHeader, recordType, color);
        // parsePipeline
        if (isError) {
            logTraceError(
                    this.traceClient,
                    parsedTraceId,
                    parsedTimestamp,
                    value,
                    "internal.error",
                    url,
                    errorMessage,
                    "KAFKA",
                    parsedPipeline,
                    parsedColor);
        } else {
            logTraceEvent(
                    this.traceClient,
                    parsedTraceId,
                    parsedTimestamp,
                    1,
                    "batch.output.send",
                    "",
                    (int) (System.currentTimeMillis() - parsedTimestamp),
                    "KAFKA",
                    parsedPipeline,
                    parsedColor);
        }
    }
}
