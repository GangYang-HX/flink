package com.bilibili.bsql.kafka.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.SourcePartitionInfoProvider;
import org.apache.flink.table.data.RowData;

/** BsqlKafkaDataStreamScanProvider. */
public class BsqlKafkaDataStreamScanProvider implements SourcePartitionInfoProvider {

    private WatermarkStrategy<RowData> watermarkStrategy;
    private final Integer numberOfTopicPartitions;
    private final KafkaSource<RowData> kafkaSource;
    private final String tableIdentifier;
    private final String kafkaTransformation;

    public BsqlKafkaDataStreamScanProvider(
            WatermarkStrategy<RowData> watermarkStrategy,
            Integer numberOfTopicPartitions,
            KafkaSource<RowData> kafkaSource,
            String tableIdentifier,
            String kafkaTransformation) {
        this.watermarkStrategy = watermarkStrategy;
        this.numberOfTopicPartitions = numberOfTopicPartitions;
        this.kafkaSource = kafkaSource;
        this.tableIdentifier = tableIdentifier;
        this.kafkaTransformation = kafkaTransformation;
    }

    @Override
    public DataStream<RowData> produceDataStream(
            ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        if (watermarkStrategy == null) {
            watermarkStrategy = WatermarkStrategy.noWatermarks();
        }
        DataStreamSource<RowData> sourceStream =
                execEnv.fromSource(
                        kafkaSource, watermarkStrategy, "KafkaSource-" + tableIdentifier);
        providerContext.generateUid(kafkaTransformation).ifPresent(sourceStream::uid);
        return sourceStream;
    }

    @Override
    public boolean isBounded() {
        return kafkaSource.getBoundedness() == Boundedness.BOUNDED;
    }

    @Override
    public int getNumberOfTopicPartitions() {
        return numberOfTopicPartitions;
    }
}
