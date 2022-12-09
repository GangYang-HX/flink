package com.bilibili.bsql.kafka.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaWriter;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import com.bilibili.bsql.common.api.sink.RetrySender;
import com.bilibili.bsql.common.api.sink.RetrySinkBase;
import com.bilibili.bsql.kafka.partitioner.RandomRetryPartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * retry kafka writer.
 *
 * @param <IN>
 */
public class RetryKafkaWriter<IN> extends KafkaWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(RetryKafkaWriter.class);

    protected transient LinkedBlockingQueue<RetryKafkaRecord> retryQueue;
    protected MailboxExecutor mailboxExecutor;
    protected Consumer<RecordMetadata> metadataConsumer;
    protected RetrySinkBase<RetryKafkaRecord> retryProducer;
    protected RetrySender<RetryKafkaRecord> retrySender;
    protected FlinkKafkaPartitioner<IN> partitioner;
    protected Boolean retry;
    protected Integer pendingRetryRecordsLimit;

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
    protected RetryKafkaWriter(
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
                recoveredStates);

        this.retryQueue = new LinkedBlockingQueue<RetryKafkaRecord>();
        this.mailboxExecutor = sinkInitContext.getMailboxExecutor();
        this.metadataConsumer = sinkInitContext.<RecordMetadata>metadataConsumer().orElse(null);
        this.partitioner = partitioner;
        this.retry = retry;
        this.pendingRetryRecordsLimit = pendingRetryRecordsLimit;
        if (this.retry) {
            this.retrySender = new KafkaRetrySender();
            this.retryProducer =
                    new RetryProducer(
                            sinkInitContext.getSubtaskId(),
                            new LinkedBlockingQueue<RetryKafkaRecord>(),
                            retrySender);
            this.retryProducer.start();
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        final ProducerRecord<byte[], byte[]> record =
                recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
        if (this.retry) {
            while (this.retryProducer.getPendingRecordsSize() > pendingRetryRecordsLimit) {
                LOG.info(
                        "kafka retry sink has too mush retrying records, slowing the process down");
                Thread.sleep(5000L);
            }
            currentProducer.send(
                    record,
                    new RetryKafkaWriter.RetryCallBack(
                            mailboxExecutor, metadataConsumer, currentProducer, null, record, 0));
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
            currentProducer.flush();
        }
    }

    /** retry callback. */
    public class RetryCallBack implements Callback {
        public ProducerRecord<byte[], byte[]> record;
        public int failedTimes;
        private final MailboxExecutor mailboxExecutor;
        @Nullable private final Consumer<RecordMetadata> metadataConsumer;
        private final KafkaProducer<byte[], byte[]> producer;
        private final String brokers;

        public RetryCallBack(
                MailboxExecutor mailboxExecutor,
                @Nullable Consumer<RecordMetadata> metadataConsumer,
                KafkaProducer<byte[], byte[]> producer,
                String brokers,
                ProducerRecord<byte[], byte[]> record,
                int failedTimes) {
            this.record = record;
            this.failedTimes = failedTimes;
            this.mailboxExecutor = mailboxExecutor;
            this.metadataConsumer = metadataConsumer;
            this.producer = producer;
            this.brokers = brokers;
        }

        protected void handleException(Exception exception) {
            if (exception instanceof RetriableException) {
                LOG.warn("write to kafka exception occurred: {}", exception.getMessage());
            } else {
                throw new RuntimeException(exception);
            }
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            // TODO origin mailboxExecutor ？？
            if (exception != null) {
                handleException(exception);
                try {
                    if (RetryKafkaWriter.this.partitioner instanceof RandomRetryPartitioner) {
                        ((RandomRetryPartitioner) RetryKafkaWriter.this.partitioner)
                                .addFailedPartition(record.partition());
                    }
                    numRecordsOutErrorsCounter.inc();
                    numRecordsSendErrorsCounter.inc();
                    retryProducer.addRetryRecord(
                            new RetryKafkaRecord(this.producer, null, record, failedTimes + 1));
                } catch (InterruptedException e) {
                    LOG.error("kafka sink put into retry queue error", e);
                }
            }

            if (metadataConsumer != null) {
                metadataConsumer.accept(metadata);
            }
        }
    }

    class RetryProducer extends RetrySinkBase<RetryKafkaRecord> {
        public RetryProducer(
                int subtaskId,
                LinkedBlockingQueue<RetryKafkaRecord> retryQueue,
                RetrySender<RetryKafkaRecord> retrySender) {
            super(subtaskId, retryQueue, retrySender);
        }
    }

    class KafkaRetrySender implements RetrySender<RetryKafkaRecord> {
        public KafkaRetrySender() {}

        @Override
        public void sendRecord(RetryKafkaRecord record) {
            ProducerRecord<byte[], byte[]> producerRecord = record.record;
            KafkaProducer<byte[], byte[]> producer = record.producer;
            byte[] serializedKey = producerRecord.key();
            byte[] serializedValue = producerRecord.value();
            Long timestamp = producerRecord.timestamp();
            Headers headers = producerRecord.headers();

            String targetTopic = producerRecord.topic();

            ProducerRecord<byte[], byte[]> newRecord =
                    new ProducerRecord<>(
                            targetTopic,
                            RetryKafkaWriter.this.partitioner.partition(
                                    null,
                                    serializedKey,
                                    serializedValue,
                                    targetTopic,
                                    RetryKafkaWriter.this.kafkaSinkContext.getPartitionsForTopic(
                                            targetTopic, record.brokers)),
                            timestamp,
                            serializedKey,
                            serializedValue,
                            headers);

            producer.send(
                    newRecord,
                    new RetryKafkaWriter.RetryCallBack(
                            mailboxExecutor,
                            metadataConsumer,
                            producer,
                            record.brokers,
                            record.record,
                            record.failedTimes));
        }

        @Override
        public void close() {}
    }
}
