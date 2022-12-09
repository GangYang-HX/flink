
package com.bilibili.bsql.kafka.diversion.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * DiversionKafkaSinkBuilder.
 *
 * @param <IN>
 */
@PublicEvolving
public class DiversionKafkaSinkBuilder<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(DiversionKafkaSinkBuilder.class);
    private static final Duration DEFAULT_KAFKA_TRANSACTION_TIMEOUT = Duration.ofHours(1);
    private static final int MAXIMUM_PREFIX_BYTES = 64000;

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
    private String transactionalIdPrefix = "kafka-sink";

    private Properties kafkaProducerConfig;
    private KafkaRecordSerializationSchema<IN> recordSerializer;
    private FlinkKafkaPartitioner<IN> partitioner;
    private String bootstrapServers;
    private Boolean retry = true;
    private Integer pendingRetryRecordsLimit;

    DiversionKafkaSinkBuilder() {}

    /**
     * Sets the wanted the {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * #deliveryGuarantee}.
     *
     * @param deliveryGuarantee
     * @return {@link KafkaSinkBuilder}
     */
    public DiversionKafkaSinkBuilder<IN> setDeliverGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        return this;
    }

    public DiversionKafkaSinkBuilder<IN> setPartitoner(FlinkKafkaPartitioner<IN> partitioner) {
        this.partitioner = checkNotNull(partitioner, "partitioner");
        return this;
    }

    public DiversionKafkaSinkBuilder<IN> setRetry(Boolean retry) {
        this.retry = retry;
        return this;
    }

    public DiversionKafkaSinkBuilder<IN> setPendingRetryRecordsLimit(
            Integer pendingRetryRecordsLimit) {
        this.pendingRetryRecordsLimit = pendingRetryRecordsLimit;
        return this;
    }

    /**
     * Sets the configuration which used to instantiate all used {@link
     * org.apache.kafka.clients.producer.KafkaProducer}.
     *
     * @param kafkaProducerConfig
     * @return {@link KafkaSinkBuilder}
     */
    public DiversionKafkaSinkBuilder<IN> setKafkaProducerConfig(Properties kafkaProducerConfig) {
        this.kafkaProducerConfig = checkNotNull(kafkaProducerConfig, "kafkaProducerConfig");
        // set the producer configuration properties for kafka record key value serializers.
        if (!kafkaProducerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            kafkaProducerConfig.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName());
        } else {
            LOG.warn(
                    "Overwriting the '{}' is not recommended",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        }

        if (!kafkaProducerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            kafkaProducerConfig.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName());
        } else {
            LOG.warn(
                    "Overwriting the '{}' is not recommended",
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }

        if (!kafkaProducerConfig.containsKey(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)) {
            final long timeout = DEFAULT_KAFKA_TRANSACTION_TIMEOUT.toMillis();
            checkState(
                    timeout < Integer.MAX_VALUE && timeout > 0,
                    "timeout does not fit into 32 bit integer");
            kafkaProducerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) timeout);
            LOG.warn(
                    "Property [{}] not specified. Setting it to {}",
                    ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                    DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
        }
        return this;
    }

    /**
     * Sets the {@link KafkaRecordSerializationSchema} that transforms incoming records to {@link
     * org.apache.kafka.clients.producer.ProducerRecord}s.
     *
     * @param recordSerializer
     * @return {@link KafkaSinkBuilder}
     */
    public DiversionKafkaSinkBuilder<IN> setRecordSerializer(
            KafkaRecordSerializationSchema<IN> recordSerializer) {
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        ClosureCleaner.clean(
                this.recordSerializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    /**
     * Sets the prefix for all created transactionalIds if {@link DeliveryGuarantee#EXACTLY_ONCE} is
     * configured.
     *
     * <p>It is mandatory to always set this value with {@link DeliveryGuarantee#EXACTLY_ONCE} to
     * prevent corrupted transactions if multiple jobs using the KafkaSink run against the same
     * Kafka Cluster. The default prefix is {@link #transactionalIdPrefix}.
     *
     * <p>The size of the prefix is capped by {@link #MAXIMUM_PREFIX_BYTES} formatted with UTF-8.
     *
     * <p>It is important to keep the prefix stable across application restarts. If the prefix
     * changes it might happen that lingering transactions are not correctly aborted and newly
     * written messages are not immediately consumable until the transactions timeout.
     *
     * @param transactionalIdPrefix
     * @return {@link KafkaSinkBuilder}
     */
    public DiversionKafkaSinkBuilder<IN> setTransactionalIdPrefix(String transactionalIdPrefix) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        checkState(
                transactionalIdPrefix.getBytes(StandardCharsets.UTF_8).length
                        <= MAXIMUM_PREFIX_BYTES,
                "The configured prefix is too long and the resulting transactionalId might exceed Kafka's transactionalIds size.");
        return this;
    }

    /**
     * Sets the Kafka bootstrap servers.
     *
     * @param bootstrapServers a comma separated list of valid URIs to reach the Kafka broker
     * @return {@link KafkaSinkBuilder}
     */
    public DiversionKafkaSinkBuilder<IN> setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = checkNotNull(bootstrapServers);
        return this;
    }

    /**
     * Constructs the {@link KafkaSink} with the configured properties.
     *
     * @return {@link KafkaSink}
     */
    public BsqlDiversionKafkaSink<IN> build() {
        if (kafkaProducerConfig == null) {
            setKafkaProducerConfig(new Properties());
        }
        checkNotNull(bootstrapServers);
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            checkState(
                    transactionalIdPrefix != null,
                    "EXACTLY_ONCE delivery guarantee requires a transactionIdPrefix to be set to provide unique transaction names across multiple KafkaSinks writing to the same Kafka cluster.");
        }
        kafkaProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new BsqlDiversionKafkaSink<>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                checkNotNull(recordSerializer, "recordSerializer"),
                partitioner,
                retry,
                pendingRetryRecordsLimit);
    }
}
