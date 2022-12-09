package com.bilibili.bsql.kafka.table;

import org.apache.flink.trace.LogTrace;
import com.bilibili.bsql.kafka.ability.metadata.read.ReadableMetadata;
import com.bilibili.bsql.kafka.util.KafkaMetadataParser;
import com.bilibili.bsql.kafka.util.LancerUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * @Author weizefeng
 * @Date 2022/3/29 15:28
 **/
public class LancerDeserializationSchema extends DynamicKafkaDeserializationSchema {

    private final LogTrace logTrace;

    public LancerDeserializationSchema(
        DeserializationSchema<RowData> valueDeserialization,
        int[] physicalColumnIndices,
        int[] metadataColumnIndices,
        int[] selectedColumns,
        ReadableMetadata.MetadataConverter[] metadataConverters,
        String sinkDest
    ) {
        super(valueDeserialization, physicalColumnIndices, metadataColumnIndices, selectedColumns, metadataConverters);
        if (valueDeserialization instanceof LogTrace) {
            this.logTrace = (LogTrace) valueDeserialization;
        } else {
            throw new UnsupportedOperationException(sinkDest + " is not supported in LancerDeserializationSchema");
        }
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector) throws Exception {
        List<ConsumerRecord<byte[], byte[]>> consumerRecords = LancerUtils.splitConsumerRecord(record);
        logTrace.setHeaders(KafkaMetadataParser.parseKafkaHeader(record.headers()));
        for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
            super.deserialize(consumerRecord, collector);
        }

    }
}
