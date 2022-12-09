package org.apache.flink.connector.kafka.blacklist;

import org.apache.flink.api.java.tuple.Tuple2;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Accumulator for producer to observe topic partition lags. */
public class ObserveLagAccumulator implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ObserveLagAccumulator.class);

    private final transient ScheduledThreadPoolExecutor observerScheduler;

    /** ((zkHost,topic),partitions). */
    private final Map<Tuple2<String, String>, List<String>> allBlacklistTp;

    /** Partitions of each topic. */
    private final Map<String, int[]> topicPartitionsMap = new ConcurrentHashMap<>();

    /** All topics that need to observe the blacklist tp (zkHost,topics). */
    private final Map<String, Set<String>> allObserveTopics;

    private final Map<String, ZkObserver> zkObservers;

    private final Map<String, Properties> allProps;

    public ObserveLagAccumulator() {
        this.observerScheduler =
                new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                                .setNameFormat("ObserveLagAccumulator-scheduler-pool")
                                .build());
        this.zkObservers = new HashMap<>();
        this.allBlacklistTp = new ConcurrentHashMap<>();
        this.allObserveTopics = new ConcurrentHashMap<>();
        this.allProps = new HashMap<>();
        this.startScheduler();
    }

    private void startScheduler() {
        this.observerScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        this.allObserveTopics.forEach(
                                (zkHost, topics) -> {
                                    if (!this.zkObservers.containsKey(zkHost)
                                            && this.allProps.containsKey(zkHost)) {
                                        this.initZkObserver(this.allProps.get(zkHost));
                                    }
                                    // Check if all topics are watched
                                    List<String> currentWatchTopics =
                                            this.zkObservers.get(zkHost).getTopics();
                                    topics.forEach(
                                            topic -> {
                                                if (!currentWatchTopics.contains(topic)) {
                                                    try {
                                                        this.zkObservers
                                                                .get(zkHost)
                                                                .addWatcher(zkHost, topic);
                                                    } catch (Exception e) {
                                                        LOG.error(
                                                                "add watcher for topic {} error, zkHost = {}",
                                                                topic,
                                                                zkHost);
                                                    }
                                                }
                                            });
                                });
                        LOG.info(
                                "current allObserveTopics = {}, allBlacklistTp = {}, zkObservers = {}",
                                this.allObserveTopics,
                                this.allBlacklistTp,
                                this.zkObservers);
                    } catch (Exception e) {
                        LOG.error(
                                "observerScheduler occur error, zkObservers = {}",
                                this.zkObservers,
                                e);
                    }
                },
                60,
                180,
                TimeUnit.SECONDS);
    }

    public void update(Properties observeInput) {
        if (observeInput.containsKey(ConsumerBlacklistTpUtils.ZK_HOST_KEY)
                && observeInput.containsKey(ConsumerBlacklistTpUtils.TOPICS_KEY)) {
            if (this.allObserveTopics.containsKey(
                    observeInput.getProperty(ConsumerBlacklistTpUtils.ZK_HOST_KEY))) {
                this.allObserveTopics
                        .get(observeInput.getProperty(ConsumerBlacklistTpUtils.ZK_HOST_KEY))
                        .addAll(
                                (List<String>)
                                        observeInput.get(ConsumerBlacklistTpUtils.TOPICS_KEY));
            } else {
                this.allObserveTopics.put(
                        observeInput.getProperty("zkHost"),
                        new HashSet<>(
                                (List<String>)
                                        observeInput.get(ConsumerBlacklistTpUtils.TOPICS_KEY)));
            }
            if (!this.allProps.containsKey(
                    observeInput.getProperty(ConsumerBlacklistTpUtils.ZK_HOST_KEY))) {
                this.allProps.put(
                        observeInput.getProperty(ConsumerBlacklistTpUtils.ZK_HOST_KEY),
                        observeInput);
            }
        }
    }

    public Map<Tuple2<String, String>, List<String>> getAllBlacklistTp() {
        this.zkObservers.forEach((k, v) -> this.allBlacklistTp.putAll(v.getObserveTps()));
        return this.allBlacklistTp;
    }

    public void initZkObserver(Properties observeInput) {
        if (observeInput.containsKey(ConsumerBlacklistTpUtils.ZK_HOST_KEY)
                && observeInput.containsKey(ConsumerBlacklistTpUtils.BLACK_LIST_PATH_KEY)) {
            String zkHost = observeInput.getProperty(ConsumerBlacklistTpUtils.ZK_HOST_KEY);
            String blacklistPath =
                    observeInput.getProperty(ConsumerBlacklistTpUtils.BLACK_LIST_PATH_KEY);
            try {
                ZkObserver zkObserver = new ZkObserver(zkHost, blacklistPath);
                this.zkObservers.put(zkHost, zkObserver);
                LOG.info(
                        "Thread {}, create zkHost {} 's zkObserver = {}",
                        Thread.currentThread(),
                        zkHost,
                        zkObserver);
            } catch (Exception e) {
                LOG.error("init zk client error, zkHost = {}", zkHost, e);
            }
        }
    }
}
