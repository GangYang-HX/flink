package org.apache.flink.connector.kafka.blacklist;

import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.connector.kafka.blacklist.ConsumerBlacklistTpUtils.BLACK_LIST_PATH_KEY;
import static org.apache.flink.connector.kafka.blacklist.ConsumerBlacklistTpUtils.JOB_ID_KEY;
import static org.apache.flink.connector.kafka.blacklist.ConsumerBlacklistTpUtils.TOPICS_KEY;
import static org.apache.flink.connector.kafka.blacklist.ConsumerBlacklistTpUtils.ZK_HOST_KEY;

/** Utils for kafka producer to observe partition lags. */
public class ProducerBlacklistTpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerBlacklistTpUtils.class);

    /** kafka tp blacklist config. */
    public static final String BLACKLIST_ZK_HOST = "blacklist.zk.host";

    public static final String BLACKLIST_LAG_THRESHOLD = "blacklist.lag.threshold";

    public static final String BLACKLIST_KICK_THRESHOLD = "blacklist.kick.threshold";

    public static final String BLACKLIST_ZK_ROOT_PATH = "blacklist.zk.root.path";

    public static void printProps(int subtaskId, String producerType, Properties producerProps) {
        Properties blacklistProps = new Properties();
        if (producerProps.containsKey(BLACKLIST_ZK_HOST)) {
            blacklistProps.put(BLACKLIST_ZK_HOST, producerProps.getProperty(BLACKLIST_ZK_HOST));
        }
        if (producerProps.containsKey(BLACKLIST_ZK_ROOT_PATH)) {
            blacklistProps.put(
                    BLACKLIST_ZK_ROOT_PATH, producerProps.getProperty(BLACKLIST_ZK_ROOT_PATH));
        }
        if (producerProps.containsKey(BLACKLIST_LAG_THRESHOLD)) {
            blacklistProps.put(
                    BLACKLIST_LAG_THRESHOLD, producerProps.getProperty(BLACKLIST_LAG_THRESHOLD));
        }
        if (producerProps.containsKey(BLACKLIST_KICK_THRESHOLD)) {
            blacklistProps.put(
                    BLACKLIST_KICK_THRESHOLD, producerProps.getProperty(BLACKLIST_KICK_THRESHOLD));
        }
        LOG.info(
                "{},subtaskId:{}, enable blacklist, blacklistProps = {}",
                producerType,
                subtaskId,
                blacklistProps);
    }

    public static Properties generateInputProps(
            Properties producerProps, List<String> topics, String jobId) {
        Properties inputProps = new Properties();
        inputProps.put(
                ZK_HOST_KEY, producerProps.getProperty(ProducerBlacklistTpUtils.BLACKLIST_ZK_HOST));
        inputProps.put(
                BLACK_LIST_PATH_KEY,
                producerProps.getProperty(ProducerBlacklistTpUtils.BLACKLIST_ZK_ROOT_PATH));
        inputProps.put(TOPICS_KEY, topics);
        inputProps.put(JOB_ID_KEY, jobId);
        return inputProps;
    }

    /** calculate each topic' knick threshold by the sum of partitions. */
    public static Map<String, Integer> getKickThresholdMap(
            Map<String, int[]> topicPartitionMap, int kickThreshold) {
        Map<String, Integer> result = new HashMap<>();
        // if user doesn't set kick threshold in bsql config, kick threshold = (partitions num)  *
        // 0.1
        if (-1 == kickThreshold) {
            topicPartitionMap.forEach(
                    (topic, partitions) -> {
                        int maxKickNum = new Double(partitions.length * 0.1).intValue();
                        result.put(topic, Math.max(maxKickNum, 1));
                    });
        } else {
            // otherwise use kick threshold in bsql config
            topicPartitionMap.forEach(
                    (topic, partitions) -> {
                        result.put(topic, kickThreshold);
                    });
        }
        return result;
    }

    /** Avoid the kick tp nums more than kick threshold. */
    public static Map<Tuple2<String, String>, List<String>> checkBlacklistTp(
            Map<Tuple2<String, String>, List<String>> allBlacklistTps,
            Map<String, Integer> kickThresholds) {
        if (null == allBlacklistTps) {
            LOG.warn("current allBlacklistTps = null");
            return new ConcurrentHashMap<>();
        }
        allBlacklistTps.forEach(
                (k, v) -> {
                    int kickThreshold = kickThresholds.getOrDefault(k.f1, 1);
                    if (v.size() > kickThreshold) {
                        allBlacklistTps.replace(k, v.subList(0, kickThreshold));
                    }
                });
        return allBlacklistTps;
    }
}
