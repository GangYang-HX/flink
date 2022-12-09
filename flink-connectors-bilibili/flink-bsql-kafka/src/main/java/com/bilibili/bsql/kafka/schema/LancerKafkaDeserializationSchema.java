package com.bilibili.bsql.kafka.schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.bilibili.bsql.kafka.util.KafkaMetadataParser;
import com.bilibili.bsql.kafka.util.LancerUtils;
import com.bilibili.bsql.trace.LogTracer;
import com.bilibili.bsql.trace.Trace;
import com.bilibili.bsql.trace.TraceUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.CUSTOM_TRACE_CLASS;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.SINK_DEST;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.TRACE_ID;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafkaConfig.TRACE_KIND;

/** LancerKafkaDeserializationSchema,use for lancer data parse and trace report. */
public class LancerKafkaDeserializationSchema extends DynamicKafkaDeserializationSchema
        implements LogTracer {

    private static final Logger LOG =
            LoggerFactory.getLogger(LancerKafkaDeserializationSchema.class);

    private LogTracer logTracer;

    private Trace traceClient;
    private String traceId;
    private String traceKind;
    private String traceClass;
    private String sinkDest;
    private Properties userDefinedProps;

    public LancerKafkaDeserializationSchema(
            int physicalArity,
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            int[] keyProjection,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            boolean upsertMode,
            Properties userDefinedProps) {
        super(
                physicalArity,
                keyDeserialization,
                keyProjection,
                valueDeserialization,
                valueProjection,
                hasMetadata,
                metadataConverters,
                producedTypeInfo,
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
        this.sinkDest =
                this.userDefinedProps
                        .getOrDefault(SINK_DEST.key(), SINK_DEST.defaultValue())
                        .toString();
        if (valueDeserialization instanceof LogTracer) {
            logTracer = (LogTracer) valueDeserialization;
            logTracer.setTraceParam(traceId, sinkDest);
        }
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        super.open(context);
        traceClient =
                Trace.createTraceClient(
                        Thread.currentThread().getContextClassLoader(),
                        this.traceKind,
                        this.traceClass,
                        this.traceId);
        LOG.info(
                "open BsqlDynamicKafkaDeserializationSchema, userDefinedProps = {}, traceClient = {}",
                this.userDefinedProps,
                this.traceClient);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector)
            throws Exception {
        if (logTracer != null) {
            Map<String, String> headers = KafkaMetadataParser.parseKafkaHeader(record.headers());
            logTracer.setHeaders(headers);
        }
        List<ConsumerRecord<byte[], byte[]>> consumerRecords =
                LancerUtils.splitConsumerRecord(record);
        for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
            traceEvent(record, traceId, sinkDest, 1, "", "lancer2-bsql", "", false, "");
            super.deserialize(consumerRecord, collector);
        }
    }

    private void traceEvent(
            ConsumerRecord record,
            String traceId,
            String sinkDest,
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
                    sinkDest,
                    parsedPipeline,
                    parsedColor);
        } else {
            logTraceEvent(
                    this.traceClient,
                    parsedTraceId,
                    parsedTimestamp,
                    1,
                    "batch.input.receive",
                    "",
                    (int) (System.currentTimeMillis() - parsedTimestamp),
                    sinkDest,
                    parsedPipeline,
                    parsedColor);
        }
    }
}
