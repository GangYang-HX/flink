package org.apache.flink.connector.kafka.blacklist;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Accumulator for each subtask to init zookeeper and update partition lag info. */
public class ReportLagAccumulator {

    private static final Logger LOG = LoggerFactory.getLogger(ReportLagAccumulator.class);
    /** (( zkHost, topic, partitionId,subtaskId), lagValue). */
    private final Map<Tuple4<String, String, Integer, Integer>, Long> allTpLags;

    /** (zkHost,topics). */
    private final Map<String, Set<String>> allReportTopics;

    /** (zkHost,zkProperties). */
    private final Map<String, Properties> allProps;

    /** (zkHost,zkReporter). */
    private final Map<String, ZkReporter> zkReporters;

    private final transient ScheduledThreadPoolExecutor registerScheduler;

    private int clearCount;

    private final String jobId;

    public ReportLagAccumulator() {
        this.allTpLags = new ConcurrentHashMap<>();
        this.zkReporters = new HashMap<>();
        this.allReportTopics = new ConcurrentHashMap<>();
        this.clearCount = 0;
        this.allProps = new HashMap<>();
        this.jobId = "";

        this.registerScheduler =
                new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                                .setNameFormat("ReportLagAccumulator-scheduler-pool")
                                .build());
        this.startScheduler();
        LOG.info("Thread {} create ReportLagAccumulator success", Thread.currentThread().getName());
    }

    private void startScheduler() {
        this.registerScheduler.scheduleWithFixedDelay(
                () -> {
                    try {
                        // Check if zkReporter has been created for all zkHosts
                        this.allReportTopics.forEach(
                                (zkHost, topics) -> {
                                    if (!this.zkReporters.containsKey(zkHost)
                                            && this.allProps.containsKey(zkHost)) {
                                        this.initZkReporter(this.allProps.get(zkHost));
                                    }
                                    // Then check if Node is created in zk for all topics
                                    List<String> currentReportTopics =
                                            this.zkReporters.get(zkHost).topics();
                                    topics.forEach(
                                            topic -> {
                                                if (!currentReportTopics.contains(topic)) {
                                                    try {
                                                        // create a zk node
                                                        if (this.zkReporters.containsKey(zkHost)) {
                                                            this.zkReporters
                                                                    .get(zkHost)
                                                                    .createZkNodeForTopic(topic);
                                                        }
                                                    } catch (Exception e) {
                                                        LOG.error(
                                                                "create zk node for topic {} error, zkHost = {}",
                                                                topic,
                                                                zkHost,
                                                                e);
                                                    }
                                                }
                                            });
                                });

                        this.zkReporters.forEach(
                                (zkHost, zkReporter) -> {
                                    this.allReportTopics
                                            .get(zkHost)
                                            .forEach(
                                                    topic -> {
                                                        zkReporter.registerBlackListTp(
                                                                topic,
                                                                filterTp(zkHost, this.allTpLags));
                                                        this.clearCount++;
                                                        if (20 == clearCount) {
                                                            this.allTpLags.clear();
                                                            this.clearCount = 0;
                                                            LOG.info("timed clean tpLags success");
                                                        }
                                                    });
                                });

                        LOG.info(
                                "current allReportTopics = {}, allTpLags = {}, allReportTopics = {},  zkReporters = {}",
                                this.allReportTopics,
                                this.allTpLags,
                                this.allReportTopics,
                                this.zkReporters);
                    } catch (Exception e) {
                        LOG.error(
                                "registerScheduler occur error, zkReporters = {}",
                                this.zkReporters,
                                e);
                    }
                },
                60,
                180,
                TimeUnit.SECONDS);
    }

    public void update(Properties reportInput) {
        String zkHostKey = ConsumerBlacklistTpUtils.ZK_HOST_KEY;
        String topicsKey = ConsumerBlacklistTpUtils.TOPICS_KEY;
        if (reportInput.containsKey(zkHostKey) && reportInput.containsKey(topicsKey)) {
            if (this.allReportTopics.containsKey(reportInput.getProperty(zkHostKey))) {
                this.allReportTopics
                        .get(reportInput.getProperty(zkHostKey))
                        .addAll((List<String>) reportInput.get(topicsKey));
            } else {
                this.allReportTopics.put(
                        reportInput.getProperty(zkHostKey),
                        new HashSet<>((List<String>) reportInput.get(topicsKey)));
            }
            if (!this.allProps.containsKey(reportInput.getProperty(zkHostKey))) {
                this.allProps.put(reportInput.getProperty(zkHostKey), reportInput);
            }
        }

        if (reportInput.containsKey(ConsumerBlacklistTpUtils.TOPIC_PARTITION_LAGS_KEY)) {
            Map<Tuple4<String, String, Integer, Integer>, Long> tpLags =
                    (Map<Tuple4<String, String, Integer, Integer>, Long>)
                            reportInput.get(ConsumerBlacklistTpUtils.TOPIC_PARTITION_LAGS_KEY);
            this.allTpLags.putAll(tpLags);
        }
    }

    private void initZkReporter(Properties reportInput) {
        if (reportInput.containsKey(ConsumerBlacklistTpUtils.ZK_HOST_KEY)
                && reportInput.containsKey(ConsumerBlacklistTpUtils.BLACK_LIST_PATH_KEY)
                && reportInput.containsKey(ConsumerBlacklistTpUtils.TOPICS_KEY)) {
            String zkHost = reportInput.getProperty(ConsumerBlacklistTpUtils.ZK_HOST_KEY);
            if (this.zkReporters.containsKey(zkHost)) {
                return;
            }
            String blacklistPath =
                    reportInput.getProperty(ConsumerBlacklistTpUtils.BLACK_LIST_PATH_KEY);
            List<String> topics =
                    (List<String>) reportInput.get(ConsumerBlacklistTpUtils.TOPICS_KEY);
            long lagThreshold =
                    Long.parseLong(
                            String.valueOf(
                                    reportInput.get(ConsumerBlacklistTpUtils.LAG_THRESHOLD_KEY)));
            int kickThreshold =
                    Integer.parseInt(
                            String.valueOf(
                                    reportInput.get(ConsumerBlacklistTpUtils.KICK_THRESHOLD_KEY)));
            int lagTimes =
                    Integer.parseInt(
                            String.valueOf(
                                    reportInput.get(ConsumerBlacklistTpUtils.LAG_TIMES_KEY)));
            ZkReporter zkReporter = null;
            try {
                zkReporter =
                        new ZkReporter(
                                zkHost,
                                blacklistPath,
                                topics,
                                jobId,
                                lagThreshold,
                                kickThreshold,
                                lagTimes);
                this.zkReporters.put(zkHost, zkReporter);
                LOG.info(
                        "Thread {}, create zkHost {} 's zkReporter = {}",
                        Thread.currentThread(),
                        zkHost,
                        zkReporter);
            } catch (Exception e) {
                LOG.error(
                        "init zk client for topic {} error, zkHost = {}, jobId = {}",
                        topics,
                        zkHost,
                        jobId,
                        e);
            }
        }
    }

    private Map<Tuple2<String, Integer>, Long> filterTp(
            String zkHost, Map<Tuple4<String, String, Integer, Integer>, Long> allTpLags) {
        Map<Tuple2<String, Integer>, Long> result = new HashMap<>();
        allTpLags.forEach(
                (tpInfo, lag) -> {
                    if (tpInfo.f0.equals(zkHost)) {
                        result.put(new Tuple2<>(tpInfo.f1, tpInfo.f2), lag);
                    }
                });
        return result;
    }
}
