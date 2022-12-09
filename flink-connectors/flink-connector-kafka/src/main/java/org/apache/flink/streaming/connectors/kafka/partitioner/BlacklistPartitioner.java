package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.blacklist.ObserveAggFunc;
import org.apache.flink.connector.kafka.blacklist.ProducerBlacklistTpUtils;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.util.CollectionUtil;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * BlacklistPartitioner.
 *
 * @param <T>
 */
public abstract class BlacklistPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final Logger LOG = LoggerFactory.getLogger(BlacklistPartitioner.class);
    /** ( zkHost, topic), blacklistTps). */
    private Map<Tuple2<String, String>, List<String>> allBlacklistTps = new ConcurrentHashMap<>();

    private String zkHost;
    private boolean enableBlacklist = false;
    private final Map<String, int[]> filteredPartitions = new ConcurrentHashMap<>();

    private GlobalAggregateManager aggregateManager;
    private StreamingRuntimeContext runtimeContext;
    private ScheduledThreadPoolExecutor aggScheduler;
    private Properties blacklistProps;
    private Integer subtaskId;

    /** Partitions of each topic. */
    protected Map<String, int[]> topicPartitionsMap = new ConcurrentHashMap<>();

    public BlacklistPartitioner() {}

    public void initBlacklistPartitioner(
            Properties userDefinedProps,
            Integer subtaskId,
            StreamingRuntimeContext runtimeContext) {
        this.zkHost = userDefinedProps.getProperty(KafkaConnectorOptions.BLACKLIST_ZK_HOST.key());
        this.blacklistProps = userDefinedProps;
        this.enableBlacklist = true;
        this.aggregateManager = runtimeContext.getGlobalAggregateManager();
        this.runtimeContext = runtimeContext;
        this.subtaskId = subtaskId;
        aggScheduler =
                new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                                .setNameFormat("RetryKafkaProducer-agg-scheduler-" + subtaskId)
                                .build());
        aggScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        this.updateAgg();
                    } catch (Exception e) {
                        LOG.error("updateAgg error, ", e);
                    }
                },
                60,
                90,
                TimeUnit.SECONDS);
        ProducerBlacklistTpUtils.printProps(subtaskId, "RetryKafkaProducer", this.blacklistProps);
    }

    // only for test
    public void initBlacklistPartitioner(Properties blacklistProps) {
        this.zkHost = blacklistProps.getProperty(KafkaConnectorOptions.BLACKLIST_ZK_HOST.key());
        this.blacklistProps = blacklistProps;
        this.enableBlacklist = true;
    }

    public void updateAgg() {
        if (CollectionUtil.isNullOrEmpty(topicPartitionsMap)) {
            LOG.info("subtaskId:{},topicPartitionsMap is empty", subtaskId);
            return;
        }
        Map<Tuple2<String, String>, List<String>> result;
        try {
            Properties inputProps =
                    ProducerBlacklistTpUtils.generateInputProps(
                            blacklistProps, new ArrayList<>(topicPartitionsMap.keySet()), "");

            Properties output =
                    this.aggregateManager.updateGlobalAggregate(
                            ObserveAggFunc.NAME, inputProps, new ObserveAggFunc());
            result =
                    (Map<Tuple2<String, String>, List<String>>)
                            output.get(ObserveAggFunc.PROPS_KEY_ALL_BLACKLIST_TPS);
            this.setAllBlacklistTps(
                    ProducerBlacklistTpUtils.checkBlacklistTp(
                            result,
                            ProducerBlacklistTpUtils.getKickThresholdMap(
                                    topicPartitionsMap,
                                    Integer.parseInt(
                                            blacklistProps.getProperty(
                                                    KafkaConnectorOptions.BLACKLIST_KICK_THRESHOLD
                                                            .key())))),
                    topicPartitionsMap);
        } catch (Exception e) {
            LOG.error("execute observe agg error, clear allBlacklistTps", e);
            setAllBlacklistTps(Maps.newHashMap(), topicPartitionsMap);
        }
    }

    public Map<Tuple2<String, String>, List<String>> allBlacklistTps() {
        return allBlacklistTps;
    }

    public void setAllBlacklistTps(
            Map<Tuple2<String, String>, List<String>> allBlacklistTps, Map<String, int[]> allTps) {
        this.allBlacklistTps = allBlacklistTps;
        allTps.forEach(
                (topic, partitions) -> {
                    this.filteredPartitions.put(topic, this.filterTp(partitions, topic));
                });
        LOG.info(
                "current filteredPartitions = {}",
                this.filteredPartitions.keySet().stream()
                        .collect(
                                Collectors.toMap(
                                        tp -> tp,
                                        tp ->
                                                Arrays.stream(this.filteredPartitions.get(tp))
                                                        .boxed()
                                                        .collect(Collectors.toList()))));
    }

    public int[] getFilteredPartitions(String targetTopic, int[] originalPartitions) {
        int[] partitions = topicPartitionsMap.get(targetTopic);
        if (null == partitions || partitions.length != originalPartitions.length) {
            topicPartitionsMap.put(targetTopic, originalPartitions);
        }
        return this.filteredPartitions.getOrDefault(targetTopic, originalPartitions);
    }

    public String zkHost() {
        return zkHost;
    }

    public boolean enableBlacklist() {
        return enableBlacklist;
    }

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        super.open(parallelInstanceId, parallelInstances);
        this.enableBlacklist = false;
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        return 0;
    }

    @Override
    public String partitionerIdentifier() {
        return super.partitionerIdentifier();
    }

    /** remove blacklist tp in old partition. */
    public int[] filterTp(int[] oldPartition, String targetTopic) {
        // 1. filter out the blacklist tp of the topic under the zkHost
        List<Integer> blacklistTps =
                this.allBlacklistTps
                        .getOrDefault(
                                new Tuple2<String, String>(this.zkHost, targetTopic),
                                Collections.emptyList())
                        .stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
        if (blacklistTps.size() == 0) {
            return oldPartition;
        }
        List<Integer> tempPartition =
                Arrays.stream(oldPartition).boxed().collect(Collectors.toList());
        tempPartition.removeIf(blacklistTps::contains);
        int[] filteredPartition = tempPartition.stream().mapToInt(i -> i).toArray();
        // this judgment guarantees data integrity in some unexpected case
        return (filteredPartition.length < oldPartition.length / 2
                        || filteredPartition.length > oldPartition.length)
                ? oldPartition
                : filteredPartition;
    }

    // only for test
    public boolean isBlacklistTp(int partition, String targetTopic) {
        if (!enableBlacklist || null == this.allBlacklistTps || null == this.zkHost) {
            return false;
        }
        List<Integer> blacklistTps =
                this.allBlacklistTps
                        .getOrDefault(
                                new Tuple2<>(this.zkHost, targetTopic), Collections.emptyList())
                        .stream()
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
        return blacklistTps.contains(partition);
    }
}
