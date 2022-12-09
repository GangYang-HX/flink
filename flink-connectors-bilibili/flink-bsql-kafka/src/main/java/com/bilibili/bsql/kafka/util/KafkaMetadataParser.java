package com.bilibili.bsql.kafka.util;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.HashMap;
import java.util.Map;

/** KafkaMetadataParser. */
public class KafkaMetadataParser {
    public static Map<String, String> parseKafkaHeader(Headers headers) {
        Map<String, String> kafkaHeaders = new HashMap<>();
        for (Header header : headers) {
            kafkaHeaders.put(header.key(), new String(header.value()));
        }
        return kafkaHeaders;
    }

    public static Map<String, byte[]> parseHeaderWithBytes(Headers headers) {
        Map<String, byte[]> kafkaHeaders = new HashMap<>();
        for (Header header : headers) {
            kafkaHeaders.put(header.key(), header.value());
        }
        return kafkaHeaders;
    }
}
