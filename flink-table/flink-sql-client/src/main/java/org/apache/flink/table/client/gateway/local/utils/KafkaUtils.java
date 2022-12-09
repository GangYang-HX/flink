package org.apache.flink.table.client.gateway.local.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** kafka util. */
public class KafkaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    private static volatile Producer<String, String> producer;
    private static String topicName;

    public KafkaUtils(Configuration configuration) {
        if (producer == null) {
            synchronized (KafkaUtils.class) {
                if (producer == null) {
                    Properties props = new Properties();
                    String bootstrapServer =
                            configuration.get(
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_MATERIALIZATION_KAFKA_BOOTSTRAP);
                    topicName =
                            configuration.get(
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_MATERIALIZATION_KAFKA_TOPIC);

                    if (StringUtils.isBlank(bootstrapServer) || StringUtils.isBlank(topicName)) {
                        String errMsg =
                                String.format(
                                        "init kafka util failed, please check config %s, %s.",
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_MATERIALIZATION_KAFKA_BOOTSTRAP,
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_MATERIALIZATION_KAFKA_TOPIC);
                        LOG.error(errMsg);
                        throw new RuntimeException(errMsg);
                    }

                    props.put("bootstrap.servers", bootstrapServer);
                    props.put("acks", "all");
                    props.put("retries", 3);
                    props.put(
                            "key.serializer",
                            "org.apache.kafka.common.serialization.StringSerializer");
                    props.put(
                            "value.serializer",
                            "org.apache.kafka.common.serialization.StringSerializer");
                    producer = new KafkaProducer<>(props);
                }
            }
        }

        registerShutdownHook();
    }

    public void sendMsg(String msg) {
        LOG.info("start send msg: '{}'.", msg);
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, msg);
        producer.send(
                record,
                (recordMetadata, e) -> {
                    if (e != null) {
                        LOG.error("async send msg failed: '{}'.", msg, e);
                    } else {
                        LOG.info("async send msg success.");
                    }
                });
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    private void registerShutdownHook() {
        Thread shutdownHook = new Thread(this::close);
        ShutdownHookUtil.addShutdownHookThread(shutdownHook, KafkaUtils.class.getSimpleName(), LOG);
    }
}
