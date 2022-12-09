package org.apache.flink.connector.kafka.blacklist;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_KICK_THRESHOLD;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_LAG_THRESHOLD;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_LAG_TIMES;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_ZK_HOST;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.BLACKLIST_ZK_ROOT_PATH;

/** Utils for consumer to monitor partition lags. */
public class ConsumerBlacklistTpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerBlacklistTpUtils.class);

    public static final String TOPICS_KEY = "topics";
    public static final String TOPIC_PARTITION_LAGS_KEY = "tpLags";
    public static final String ZK_HOST_KEY = "zkHost";
    public static final String BLACK_LIST_PATH_KEY = "blacklistPath";
    public static final String LAG_THRESHOLD_KEY = "lagThreshold";
    public static final String KICK_THRESHOLD_KEY = "kickThreshold";
    public static final String LAG_TIMES_KEY = "lagTimes";
    public static final String JOB_ID_KEY = "jobId";

    private static final String METRIC_KEY_TOPIC = "topic";
    private static final String METRIC_KEY_LAG = "lag";
    private static final String METRIC_KEY_MAX = "max";
    private static final String METRIC_KEY_AVG = "avg";
    private static final String METRIC_KEY_PARTITION = "partition";

    public static void printProps(int subtaskId, Configuration consumerProps) {
        Properties blacklistProps = new Properties();
        if (consumerProps.containsKey(BLACKLIST_ZK_HOST.key())) {
            blacklistProps.put(ZK_HOST_KEY, consumerProps.get(BLACKLIST_ZK_HOST));
        }
        if (consumerProps.containsKey(BLACKLIST_ZK_ROOT_PATH.key())) {
            blacklistProps.put(BLACK_LIST_PATH_KEY, consumerProps.get(BLACKLIST_ZK_ROOT_PATH));
        }
        if (consumerProps.containsKey(BLACKLIST_LAG_THRESHOLD.key())) {
            blacklistProps.put(LAG_THRESHOLD_KEY, consumerProps.get(BLACKLIST_LAG_THRESHOLD));
        }
        if (consumerProps.containsKey(BLACKLIST_KICK_THRESHOLD.key())) {
            blacklistProps.put(KICK_THRESHOLD_KEY, consumerProps.get(BLACKLIST_KICK_THRESHOLD));
        }
        if (consumerProps.containsKey(BLACKLIST_LAG_TIMES.key())) {
            blacklistProps.put(LAG_TIMES_KEY, consumerProps.get(BLACKLIST_LAG_TIMES));
        }
        LOG.info(
                "enable tp blacklist,subtaskId:{}, blacklistProps = {}", subtaskId, blacklistProps);
    }

    public static Map<TopicPartition, Long> monitorTpLags(
            int subtaskId, Set<String> subscribedTopics, Map<MetricName, KafkaMetric> metrics) {
        Map<TopicPartition, Long> tpLags = new HashMap<>();
        try {
            List<Map.Entry<MetricName, KafkaMetric>> lagMetrics =
                    metrics.entrySet().stream()
                            .filter(
                                    entry ->
                                            entry.getKey().name().contains(METRIC_KEY_LAG)
                                                    && !entry.getKey()
                                                            .name()
                                                            .contains(METRIC_KEY_MAX)
                                                    && !entry.getKey()
                                                            .name()
                                                            .contains(METRIC_KEY_AVG))
                            .collect(Collectors.toList());

            for (Map.Entry<MetricName, KafkaMetric> entry : lagMetrics) {
                MetricName key = entry.getKey();
                KafkaMetric value = entry.getValue();
                String topic = key.tags().get(METRIC_KEY_TOPIC);
                for (String subscribedTopic : subscribedTopics) {
                    if (subscribedTopic.replaceAll("\\.", "_").equals(topic)) {
                        topic = subscribedTopic;
                    }
                }
                TopicPartition topicPartition =
                        new TopicPartition(
                                topic, Integer.parseInt(key.tags().get(METRIC_KEY_PARTITION)));
                Double lag =
                        value.measurable().measure(new MetricConfig(), System.currentTimeMillis());
                tpLags.put(topicPartition, lag.longValue());
            }
        } catch (Exception e) {
            LOG.error("subtask {} monitorTpLags success, metrics = {}}", subtaskId, metrics, e);
        }

        return tpLags;
    }

    public static Properties generateInputProps(
            Configuration config, Map<TopicPartition, Long> tpLags, Integer subtaskId) {
        Properties inputProps = new Properties();
        inputProps.put(
                TOPICS_KEY,
                tpLags.keySet().stream().map(TopicPartition::topic).collect(Collectors.toList()));
        inputProps.put(ZK_HOST_KEY, config.get(BLACKLIST_ZK_HOST));
        inputProps.put(BLACK_LIST_PATH_KEY, config.get(BLACKLIST_ZK_ROOT_PATH));
        inputProps.put(LAG_THRESHOLD_KEY, config.get(BLACKLIST_LAG_THRESHOLD));
        inputProps.put(KICK_THRESHOLD_KEY, config.get(BLACKLIST_KICK_THRESHOLD));
        inputProps.put(LAG_TIMES_KEY, config.get(BLACKLIST_LAG_TIMES));
        inputProps.put(
                TOPIC_PARTITION_LAGS_KEY,
                tpLags.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        topicPartition ->
                                                new Tuple4<>(
                                                        config.get(BLACKLIST_ZK_HOST),
                                                        topicPartition.getKey().topic(),
                                                        topicPartition.getKey().partition(),
                                                        subtaskId),
                                        Map.Entry::getValue)));
        return inputProps;
    }
}
