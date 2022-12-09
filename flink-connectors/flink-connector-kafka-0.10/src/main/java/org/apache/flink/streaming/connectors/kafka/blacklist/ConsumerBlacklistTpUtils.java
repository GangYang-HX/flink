package org.apache.flink.streaming.connectors.kafka.blacklist;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010Fetcher;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author weizefeng
 * @Date 2022/2/27 13:10
 **/
public class ConsumerBlacklistTpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerBlacklistTpUtils.class);

    public static final String BLACKLIST_ENABLE = "blacklist.enable";

    public static final String BLACKLIST_ZK_HOST = "blacklist.zk.host";

    public static final String BLACKLIST_ZK_ROOT_PATH = "blacklist.zk.root.path";

    public static final String BLACKLIST_LAG_THRESHOLD = "blacklist.lag.threshold";

    public static final String BLACKLIST_KICK_THRESHOLD = "blacklist.kick.threshold";

    public static final String BLACKLIST_LAG_TIMES = "blacklist.lag.times";

    public static void printProps(Properties consumerProps) {
        Properties blacklistProps = new Properties();
        if (consumerProps.containsKey(BLACKLIST_ZK_HOST)) {
            blacklistProps.put(BLACKLIST_ZK_HOST, consumerProps.getProperty(BLACKLIST_ZK_HOST));
        }
        if (consumerProps.containsKey(BLACKLIST_ZK_ROOT_PATH)) {
            blacklistProps.put(BLACKLIST_ZK_ROOT_PATH, consumerProps.getProperty(BLACKLIST_ZK_ROOT_PATH));
        }
        if (consumerProps.containsKey(BLACKLIST_LAG_THRESHOLD)) {
            blacklistProps.put(BLACKLIST_LAG_THRESHOLD, consumerProps.getProperty(BLACKLIST_LAG_THRESHOLD));
        }
        if (consumerProps.containsKey(BLACKLIST_KICK_THRESHOLD)) {
            blacklistProps.put(BLACKLIST_KICK_THRESHOLD, consumerProps.getProperty(BLACKLIST_KICK_THRESHOLD));
        }
        if (consumerProps.containsKey(BLACKLIST_LAG_TIMES)) {
            blacklistProps.put(BLACKLIST_LAG_TIMES, consumerProps.getProperty(BLACKLIST_LAG_TIMES));
        }
        LOG.info("FlinkKafkaConsumer010 enable tp blacklist, blacklistProps = {}", blacklistProps);
    }

    public static Properties generateInputProps(
        Properties consumerProps,
        List<String> topics,
        String jobId,
        Map<TopicPartition, Long> tpLags
    ) {
        Properties inputProps = new Properties();
        inputProps.put("zkHost", consumerProps.get(ConsumerBlacklistTpUtils.BLACKLIST_ZK_HOST));
        inputProps.put("blacklistPath", consumerProps.getOrDefault(ConsumerBlacklistTpUtils.BLACKLIST_ZK_ROOT_PATH, "/blacklist"));
        if (consumerProps.containsKey(ConsumerBlacklistTpUtils.BLACKLIST_LAG_THRESHOLD)) {
            inputProps.put("lagThreshold", consumerProps.getProperty(ConsumerBlacklistTpUtils.BLACKLIST_LAG_THRESHOLD));
        }
        if (consumerProps.containsKey(ConsumerBlacklistTpUtils.BLACKLIST_KICK_THRESHOLD)) {
            inputProps.put("kickThreshold", consumerProps.getProperty(ConsumerBlacklistTpUtils.BLACKLIST_KICK_THRESHOLD));
        }
        if (consumerProps.containsKey(ConsumerBlacklistTpUtils.BLACKLIST_LAG_TIMES)) {
            inputProps.put("lagTimes", consumerProps.getProperty(ConsumerBlacklistTpUtils.BLACKLIST_LAG_TIMES));
        }
        inputProps.put("topics", topics);
        inputProps.put("jobId", jobId);
        inputProps.put("tpLags", tpLags.entrySet().stream().collect(Collectors.toMap(
            topicPartition -> new Tuple3<String, String, Integer>(
                consumerProps.getProperty(ConsumerBlacklistTpUtils.BLACKLIST_ZK_HOST),
                topicPartition.getKey().topic(),
                topicPartition.getKey().partition()),
            Map.Entry::getValue
        )));
        return inputProps;
    }

    public static Map<TopicPartition, Long> monitorTpLags(
        Map<MetricName, KafkaMetric> metrics,
        List<TopicPartition> subscribedTp,
        int subtaskId
    ) {
        Map<TopicPartition, Long> tpLags = new HashMap<>();
        Map<String, Double> lagMetrics = new HashMap<>();
        try {
            lagMetrics = metrics.entrySet()
                .stream()
                .filter(entry -> entry.getKey().name().contains("lag") &&
                    !entry.getKey().name().contains("max") &&
                    !entry.getKey().name().contains("avg"))
                .collect(Collectors.toMap(
                    entry -> entry.getKey().name(),
                    entry -> entry.getValue().measurable().measure(new MetricConfig(), System.currentTimeMillis())
                ));
            for (TopicPartition tp : subscribedTp) {
                if (lagMetrics.containsKey(tp.toString() + ".records-lag")) {
                    tpLags.put(tp, lagMetrics.get(tp.toString() + ".records-lag").longValue());
                }
            }
            LOG.info("subtask {} monitorTpLags success, lagMetrics = {}, tpLags = {}", subtaskId, lagMetrics, tpLags);
        } catch (Exception e) {
            LOG.error("subtask {} monitorTpLags error, subscribedTp = {}, lagMetrics = {}", subtaskId, subscribedTp, lagMetrics, e);
        }
        return tpLags;
    }

    public static String getJobId(StreamingRuntimeContext runtimeContext) {
        String jobId = "";
        try {
            Class<?> StreamClass = Class.forName("org.apache.flink.streaming.api.operators.StreamingRuntimeContext");
            Field taskEnvironment = StreamClass.getDeclaredField("taskEnvironment");
            taskEnvironment.setAccessible(true);
            Environment environment = (Environment) taskEnvironment.get(runtimeContext);
            return environment.getJobID().toString();
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            LOG.error("get JobId error", e);
        }
        return jobId;
    }

    @Deprecated
    public static Map<TopicPartition, Long> monitorTpLags(AbstractFetcher fetcher) {
        Map<TopicPartition, Long> tpLags = null;
        try {
            Field consumerThreadField = ((Kafka010Fetcher) fetcher).getClass().getDeclaredField("consumerThread");

            consumerThreadField.setAccessible(true);
            KafkaConsumerThread consumerThread = (KafkaConsumerThread) consumerThreadField.get(fetcher);

            Field hasAssignedPartitionsField = consumerThread.getClass().getDeclaredField("hasAssignedPartitions");
            hasAssignedPartitionsField.setAccessible(true);
            boolean hasAssignedPartitions = (boolean) hasAssignedPartitionsField.get(consumerThread);
            if (!hasAssignedPartitions) {
                LOG.warn("consumer = {} doesn't assign partition", consumerThread);
                return null;
            }

            Field consumerField = consumerThread.getClass().getDeclaredField("consumer");
            consumerField.setAccessible(true);
            KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerField.get(consumerThread);
            Field subscriptionStateField = kafkaConsumer.getClass().getDeclaredField("subscriptions");
            subscriptionStateField.setAccessible(true);
            SubscriptionState subscriptionState = (SubscriptionState) subscriptionStateField.get(kafkaConsumer);
            Set<TopicPartition> assignedPartitions = subscriptionState.assignedPartitions();

            Method[] methods = subscriptionState.getClass().getMethods();
            Method partitonLag = null;
            for (Method method : methods) {
                if (method.getName().equals("partitionLag")) {
                    partitonLag = method;
                }
            }

            if (partitonLag != null) {
                tpLags = new HashMap<>();
                for (TopicPartition topicPartition : assignedPartitions) {
                    tpLags.put(topicPartition, (Long) partitonLag.invoke(subscriptionState, topicPartition, null));
                }
            }
            LOG.info("monitorTpLags success, current lags = {}", tpLags);
        } catch (Exception e) {
            LOG.error("monitorTpLags error", e);
            return null;
        }
        return tpLags;
    }
}
