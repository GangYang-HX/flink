
package com.bilibili.bsql.kafka.diversion.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.MetricUtil;
import org.apache.flink.connector.kafka.sink.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricMutableWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import com.bilibili.bsql.kafka.sink.RetryKafkaWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * DiversionKafkaWriter.
 *
 * @param <IN>
 */
public class DiversionKafkaWriter<IN> extends RetryKafkaWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(DiversionKafkaWriter.class);
    private static final String KAFKA_PRODUCER_METRIC_NAME = "KafkaProducer";
    private static final long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    private static final String KEY_DISABLE_METRICS = "flink.disable-metrics";
    private static final String KEY_REGISTER_METRICS = "register.producer.metrics";
    private static final String KAFKA_PRODUCER_METRICS = "producer-metrics";

    private final Map<String, KafkaMetricMutableWrapper> previouslyCreatedMetrics = new HashMap<>();

    // Number of outgoing bytes at the latest metric sync
    private long latestOutgoingByteTotal;
    private Metric byteOutMetric;
    // private FlinkKafkaInternalProducer<byte[], byte[]> currentProducer;
    // producer pool only used for exactly once
    private final Deque<FlinkKafkaInternalProducer<byte[], byte[]>> producerPool =
            new ArrayDeque<>();
    private final Closer closer = Closer.create();
    private long lastCheckpointId;

    private boolean closed = false;
    private long lastSync = System.currentTimeMillis();
    Collection<KafkaWriterState> recoveredStates;
    private final Map<String, FlinkKafkaInternalProducer<byte[], byte[]>> cache = new HashMap<>(8);

    /**
     * Constructor creating a kafka writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * KafkaRecordSerializationSchema#open(SerializationSchema.InitializationContext,
     * KafkaRecordSerializationSchema.KafkaSinkContext)} fails.
     *
     * @param deliveryGuarantee the Sink's delivery guarantee
     * @param kafkaProducerConfig the properties to configure the {@link FlinkKafkaInternalProducer}
     * @param transactionalIdPrefix used to create the transactionalIds
     * @param sinkInitContext context to provide information about the runtime environment
     * @param recordSerializer serialize to transform the incoming records to {@link ProducerRecord}
     * @param schemaContext context used to initialize the {@link KafkaRecordSerializationSchema}
     * @param recoveredStates state from an previous execution which was covered
     */
    DiversionKafkaWriter(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            Sink.InitContext sinkInitContext,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            SerializationSchema.InitializationContext schemaContext,
            Collection<KafkaWriterState> recoveredStates,
            FlinkKafkaPartitioner<IN> partitioner,
            Boolean retry,
            Integer pendingRetryRecordsLimit) {
        super(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                sinkInitContext,
                recordSerializer,
                schemaContext,
                recoveredStates,
                partitioner,
                retry,
                pendingRetryRecordsLimit);
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        final ProducerRecord<byte[], byte[]> record =
                recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
        String brokers = recordSerializer.getBrokers(element);
        FlinkKafkaInternalProducer<byte[], byte[]> currentProducer;
        if (brokers != null) {
            currentProducer = getProducer(brokers);
        } else {
            throw new RuntimeException("diversion kafka sink brokers is not null!");
        }

        if (this.retry) {
            while (this.retryProducer.getPendingRecordsSize() > pendingRetryRecordsLimit) {
                LOG.info(
                        "kafka retry sink has too mush retrying records, slowing the process down");
                Thread.sleep(5000L);
            }
            currentProducer.send(
                    record,
                    new RetryCallBack(
                            mailboxExecutor,
                            metadataConsumer,
                            currentProducer,
                            brokers,
                            record,
                            0));
        } else {
            currentProducer.send(record, deliveryCallback);
        }

        numRecordsSendCounter.inc();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        Long flushTriggerTime = System.currentTimeMillis();
        while (true) {
            if ((System.currentTimeMillis() - flushTriggerTime) > 8 * 60 * 1000) {
                LOG.error(
                        "flush last for too much time {} to {}",
                        flushTriggerTime,
                        System.currentTimeMillis());
            }

            if (this.retryProducer.getPendingRecordsSize() == 0) {
                LOG.info("all the pending retry record released");
                break;
            }
            Thread.sleep(200L);
        }

        if (deliveryGuarantee != DeliveryGuarantee.NONE || endOfInput) {
            LOG.debug("final flush={}", endOfInput);
            flushAllProducers();
        }
    }

    FlinkKafkaInternalProducer<byte[], byte[]> getProducer(String brokers) {
        FlinkKafkaInternalProducer<byte[], byte[]> kafkaProducer = cache.get(brokers);
        if (kafkaProducer == null) {
            kafkaProducerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
                abortLingeringTransactions(
                        checkNotNull(recoveredStates, "recoveredStates"), lastCheckpointId + 1);
                kafkaProducer = getTransactionalProducer(lastCheckpointId + 1);
                kafkaProducer.beginTransaction();
            } else if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE
                    || deliveryGuarantee == DeliveryGuarantee.NONE) {
                kafkaProducer = new FlinkKafkaInternalProducer<>(this.kafkaProducerConfig, null);
                closer.register(kafkaProducer);
                initKafkaMetrics(kafkaProducer);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + this.deliveryGuarantee);
            }
            initFlinkMetrics(kafkaProducer);
            cache.put(brokers, kafkaProducer);
        }
        return kafkaProducer;
    }

    private void flushAllProducers() {
        for (Map.Entry<String, FlinkKafkaInternalProducer<byte[], byte[]>> entry :
                cache.entrySet()) {
            FlinkKafkaInternalProducer<byte[], byte[]> producer = entry.getValue();
            producer.flush();
        }
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            for (Map.Entry<String, FlinkKafkaInternalProducer<byte[], byte[]>> entry :
                    cache.entrySet()) {
                FlinkKafkaInternalProducer<byte[], byte[]> currentProducer;
                currentProducer = getTransactionalProducer(checkpointId + 1);
                currentProducer.beginTransaction();
            }
        }
        return ImmutableList.of(kafkaWriterState);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        for (Map.Entry<String, FlinkKafkaInternalProducer<byte[], byte[]>> entry :
                cache.entrySet()) {
            FlinkKafkaInternalProducer<byte[], byte[]> currentProducer = entry.getValue();
            LOG.debug("Closing writer with {}", currentProducer);
            closeAll(
                    () -> abortCurrentProducer(currentProducer),
                    closer,
                    producerPool::clear,
                    currentProducer);
            checkState(currentProducer.isClosed());
        }
    }

    private void abortCurrentProducer(FlinkKafkaInternalProducer<byte[], byte[]> currentProducer) {
        if (currentProducer.isInTransaction()) {
            try {
                currentProducer.abortTransaction();
            } catch (ProducerFencedException e) {
                LOG.debug(
                        "Producer {} fenced while aborting", currentProducer.getTransactionalId());
            }
        }
    }

    @VisibleForTesting
    Deque<FlinkKafkaInternalProducer<byte[], byte[]>> getProducerPool() {
        return producerPool;
    }

    private FlinkKafkaInternalProducer<byte[], byte[]> getOrCreateTransactionalProducer(
            String transactionalId) {
        FlinkKafkaInternalProducer<byte[], byte[]> producer = producerPool.poll();
        if (producer == null) {
            producer = new FlinkKafkaInternalProducer<>(kafkaProducerConfig, transactionalId);
            closer.register(producer);
            producer.initTransactions();
            initKafkaMetrics(producer);
        } else {
            producer.initTransactionId(transactionalId);
        }
        return producer;
    }

    private void initFlinkMetrics(FlinkKafkaInternalProducer<byte[], byte[]> currentProducer) {
        metricGroup.setCurrentSendTimeGauge(() -> computeSendTime(currentProducer));
        registerMetricSync();
    }

    private void initKafkaMetrics(FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        byteOutMetric =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), KAFKA_PRODUCER_METRICS, "outgoing-byte-total");
        if (disabledMetrics) {
            return;
        }
        final MetricGroup kafkaMetricGroup = metricGroup.addGroup(KAFKA_PRODUCER_METRIC_NAME);
        producer.metrics().entrySet().forEach(initMetric(kafkaMetricGroup));
    }

    private Consumer<Map.Entry<MetricName, ? extends Metric>> initMetric(
            MetricGroup kafkaMetricGroup) {
        return (entry) -> {
            final String name = entry.getKey().name();
            final Metric metric = entry.getValue();
            if (previouslyCreatedMetrics.containsKey(name)) {
                final KafkaMetricMutableWrapper wrapper = previouslyCreatedMetrics.get(name);
                wrapper.setKafkaMetric(metric);
            } else {
                final KafkaMetricMutableWrapper wrapper = new KafkaMetricMutableWrapper(metric);
                previouslyCreatedMetrics.put(name, wrapper);
                kafkaMetricGroup.gauge(name, wrapper);
            }
        };
    }

    private long computeSendTime(FlinkKafkaInternalProducer<byte[], byte[]> currentProducer) {
        if (currentProducer == null) {
            return -1L;
        }
        final Metric sendTime =
                MetricUtil.getKafkaMetric(
                        currentProducer.metrics(), KAFKA_PRODUCER_METRICS, "request-latency-avg");
        final Metric queueTime =
                MetricUtil.getKafkaMetric(
                        currentProducer.metrics(), KAFKA_PRODUCER_METRICS, "record-queue-time-avg");
        return ((Number) sendTime.metricValue()).longValue()
                + ((Number) queueTime.metricValue()).longValue();
    }

    private void registerMetricSync() {
        timeService.registerTimer(
                lastSync + METRIC_UPDATE_INTERVAL_MILLIS,
                (time) -> {
                    if (closed) {
                        return;
                    }
                    long outgoingBytesUntilNow = ((Number) byteOutMetric.metricValue()).longValue();
                    long outgoingBytesSinceLastUpdate =
                            outgoingBytesUntilNow - latestOutgoingByteTotal;
                    numBytesSendCounter.inc(outgoingBytesSinceLastUpdate);
                    latestOutgoingByteTotal = outgoingBytesUntilNow;
                    lastSync = time;
                    registerMetricSync();
                });
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() {
        if (deliveryGuarantee != DeliveryGuarantee.NONE) {
            flushAllProducers();
        }
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            List<KafkaCommittable> committables = new ArrayList<>();
            for (Map.Entry<String, FlinkKafkaInternalProducer<byte[], byte[]>> entry :
                    cache.entrySet()) {
                FlinkKafkaInternalProducer<byte[], byte[]> producer = entry.getValue();
                committables.add(KafkaCommittable.of(producer, producerPool::add));
            }
            LOG.debug("Committing {} committables", committables);
            return committables;
        }
        return Collections.emptyList();
    }
}
