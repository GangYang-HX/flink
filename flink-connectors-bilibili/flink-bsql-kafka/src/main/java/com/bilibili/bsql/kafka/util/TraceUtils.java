package com.bilibili.bsql.kafka.util;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.HashMap;
import java.util.Map;

/** Trace Utils class. */
public class TraceUtils {
    public static Map<String, String> parseKafkaHeader(Headers headers) {
        Map<String, String> kafkaHeaders = new HashMap<>();
        for (Header header : headers) {
            kafkaHeaders.put(header.key(), new String(header.value()));
        }
        return kafkaHeaders;
    }

    public static int parseRecordType(Map<String, String> headers, String traceId) {
        if (null != traceId && traceId.length() >= 6 && traceId.length() < 10) {
            return RecordType.LANCER_RECORD.recordType;
        } else if (headers.containsKey("host_name")
                && headers.containsKey("db_name")
                && headers.containsKey("table_name")) {
            return RecordType.CDC_RECORD.recordType;
        } else {
            return RecordType.OTHER_RECORD.recordType;
        }
    }

    public static String parseTraceId(Map<String, String> headers, int recordType, String traceId) {
        if (recordType == RecordType.CDC_RECORD.recordType
                && headers.containsKey("host_name")
                && headers.containsKey("db_name")
                && headers.containsKey("table_name")) {
            return headers.get("host_name")
                    + "-"
                    + headers.get("db_name")
                    + "-"
                    + headers.get("table_name");
        } else {
            return traceId;
        }
    }

    public static long parseTimestamp(
            Map<String, String> headers, int recordType, ProducerRecord record) {
        if (recordType == RecordType.LANCER_RECORD.recordType) {
            return headers.containsKey("ctime")
                    ? Long.parseLong(headers.get("ctime"))
                    : System.currentTimeMillis();
        } else if (recordType == RecordType.CDC_RECORD.recordType) {
            return record.timestamp();
        } else {
            return System.currentTimeMillis();
        }
    }

    public static String parsePipeline(int recordType, String pipeline) {
        if (recordType == RecordType.LANCER_RECORD.recordType) {
            return "lancer2-bsql";
        } else if (recordType == RecordType.CDC_RECORD.recordType) {
            return "diversion-sink";
        } else {
            return pipeline;
        }
    }

    public static String parseColor(Map<String, String> headers, int recordType, String color) {
        if (recordType == RecordType.CDC_RECORD.recordType) {
            return Boolean.parseBoolean(headers.getOrDefault("heartbeat", "false"))
                    ? "heartbeat"
                    : "biz";
        } else {
            return color;
        }
    }

    enum RecordType {
        LANCER_RECORD(1, "lancer record"),

        CDC_RECORD(2, "cdc record"),

        OTHER_RECORD(3, "other record");

        private int recordType;
        private String recordDesc;

        RecordType(int codeType, String recordDesc) {
            this.recordType = codeType;
            this.recordDesc = recordDesc;
        }
    }
}
