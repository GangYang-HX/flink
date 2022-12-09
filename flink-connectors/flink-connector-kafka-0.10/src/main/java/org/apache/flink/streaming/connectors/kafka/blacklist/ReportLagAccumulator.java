package org.apache.flink.streaming.connectors.kafka.blacklist;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @Author weizefeng
 * @Date 2022/2/23 15:24
 **/
public class ReportLagAccumulator implements Serializable {

    private final static Logger LOG = LoggerFactory.getLogger(ReportLagAccumulator.class);

    /**
     * << zkHost, topic, partitionId>, lagValue>
     */
    private Map<Tuple3<String, String, Integer>, Long> allTpLags;

    private Map<String, ZkReporter> zkReporters;


    /**
     *  所有需要注册lag的topic
     *  zkHost -> topics
     */
    private Map<String, Set<String>> allReportTopics;

    private transient ScheduledThreadPoolExecutor registerScheduler;

    private int clearCount;

    private Map<String, Properties> allProps;

    public ReportLagAccumulator() {
        this.allTpLags = new ConcurrentHashMap<>();
        this.zkReporters = new HashMap<>();
        this.allReportTopics = new ConcurrentHashMap<>();
        this.clearCount = 0;
        this.allProps = new HashMap<>();

        this.registerScheduler = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("ReportLagAccumulator-scheduler-pool").build());
        this.startScheduler();
        LOG.info("Thread {} create ReportLagAccumulator success", Thread.currentThread().getName());
    }

    private void startScheduler(){
        this.registerScheduler.scheduleWithFixedDelay(()->{
            try {
                // 检查是否已经为所有zkHost创建zkReporter
                this.allReportTopics.forEach((zkHost, topics) ->{
                    if (!this.zkReporters.containsKey(zkHost) && this.allProps.containsKey(zkHost)){
                        this.initZkReporter(this.allProps.get(zkHost));
                    }
                    // 然后再检查是否为所有topic在zk中创建Node
                    List<String> currentReportTopics = this.zkReporters.get(zkHost).topics();
                    topics.forEach(topic -> {
                        if (!currentReportTopics.contains(topic)){
                            try {
                                // 如果没有则补创建zkNode
                                if (this.zkReporters.containsKey(zkHost)){
                                    this.zkReporters.get(zkHost).createZkNodeForTopic(topic);
                                }
                            } catch (Exception e) {
                                LOG.error("create zk node for topic {} error, zkHost = {}", topic, zkHost, e);
                            }
                        }
                    });
                });

                this.zkReporters.forEach((zkHost, zkReporter) -> {
                    this.allReportTopics.get(zkHost).forEach(topic -> {
                        zkReporter.registerBlackListTp(topic, filterTp(zkHost, this.allTpLags));
                        this.clearCount++;
                        if (20 == clearCount){
                            this.allTpLags.clear();
                            this.clearCount = 0;
                            LOG.info("timed clean tpLags success");
                        }
                    });
                });

                LOG.info("current allReportTopics = {}, allTpLags = {}, allReportTopics = {},  zkReporters = {}"
                    , this.allReportTopics, this.allTpLags, this.allReportTopics, this.zkReporters);
            } catch (Exception e) {
                LOG.error("registerScheduler occur error, zkReporters = {}", this.zkReporters, e);
            }
        }, 60, 180, TimeUnit.SECONDS);
    }

    public void update(Properties reportInput){
        if (reportInput.containsKey("zkHost") && reportInput.containsKey("topics")){
            if (this.allReportTopics.containsKey(reportInput.getProperty("zkHost"))){
                this.allReportTopics.get(reportInput.getProperty("zkHost")).addAll((List<String>)reportInput.get("topics"));
            }else {
                this.allReportTopics.put(reportInput.getProperty("zkHost"), new HashSet<>((List<String>)reportInput.get("topics")));
            }
            if (!this.allProps.containsKey(reportInput.getProperty("zkHost"))){
                this.allProps.put(reportInput.getProperty("zkHost"), reportInput);
            }
        }

        if (reportInput.containsKey("tpLags")){
            this.allTpLags.putAll((Map<Tuple3<String, String,Integer>, Long>) reportInput.get("tpLags"));
        }
    }

    public void initZkReporter(Properties reportInput){
        if (reportInput.containsKey("zkHost") &&
                reportInput.containsKey("blacklistPath") &&
                reportInput.containsKey("topics") &&
                reportInput.containsKey("jobId")){
            String zkHost = reportInput.getProperty("zkHost");
            if (this.zkReporters.containsKey(zkHost)){
                return;
            }
            String blacklistPath = reportInput.getProperty("blacklistPath");
            List<String> topics = (List<String>)reportInput.get("topics");
            String jobId = reportInput.getProperty("jobId");
            long lagThreshold = Long.parseLong(reportInput.getProperty("lagThreshold"));
            int kickThreshold = Integer.parseInt(reportInput.getProperty("kickThreshold"));
            int lagTimes = Integer.parseInt(reportInput.getProperty("lagTimes"));
            ZkReporter zkReporter = null;
            try {
                zkReporter = new ZkReporter(zkHost, blacklistPath, topics, jobId, lagThreshold, kickThreshold, lagTimes);
                this.zkReporters.put(zkHost, zkReporter);
                LOG.info("Thread {}, create zkHost {} 's zkReporter = {}", Thread.currentThread(), zkHost, zkReporter);
            } catch (Exception e) {
                LOG.error("init zk client for topic {} error, zkHost = {}, jobId = {}", topics, zkHost, jobId, e);
            }
        }
    }

    private Map<Tuple2<String, Integer>, Long> filterTp(String zkHost, Map<Tuple3<String, String, Integer>, Long> allTpLags){
        Map<Tuple2<String, Integer>, Long> result = new HashMap<>();
        allTpLags.forEach((tpInfo, lag) -> {
            if (tpInfo.f0.equals(zkHost)){
                result.put(new Tuple2<>(tpInfo.f1, tpInfo.f2), lag);
            }
        });
        return result;
    }
}
