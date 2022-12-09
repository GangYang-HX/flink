package com.bilibili.bsql.kafka.blacklist;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

/**
 * @Author weizefeng
 * @Date 2022/2/26 20:27
 **/
public class ObserveLagAccumulator implements Serializable {

    private final static Logger LOG = LoggerFactory.getLogger(ObserveLagAccumulator.class);

    private transient ScheduledThreadPoolExecutor observerScheduler;

    private Map<Tuple2<String, String>, List<String>> allBlacklistTp;

    /**
     *  所有需要观察黑名单tp的topic
     *  zkHost -> topics
     */
    private Map<String, Set<String>> allObserveTopics;

    private Map<String, ZkObserver> zkObservers;

    private Map<String, Properties> allProps;

    public ObserveLagAccumulator() {
        this.observerScheduler = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("ObserveLagAccumulator-scheduler-pool").build());
        this.zkObservers = new HashMap<>();
        this.allBlacklistTp = new ConcurrentHashMap<>();
        this.allObserveTopics = new ConcurrentHashMap<>();
        this.allProps = new HashMap<>();
        this.startScheduler();
    }

    private void startScheduler(){
        this.observerScheduler.scheduleWithFixedDelay(()->{
            try {
                this.allObserveTopics.forEach((zkHost, topics) ->{
                    if (!this.zkObservers.containsKey(zkHost) && this.allProps.containsKey(zkHost)){
                        this.initZkObserver(this.allProps.get(zkHost));
                    }
                    // 检查是否watch了所有topic
                    List<String> currentWatchTopics = this.zkObservers.get(zkHost).getTopics();
                    topics.forEach(topic -> {
                        if (!currentWatchTopics.contains(topic)){
                            try {
                                this.zkObservers.get(zkHost).addWatcher(zkHost, topic);
                            } catch (Exception e) {
                                LOG.error("add watcher for topic {} error, zkHost = {}", topic, zkHost);
                            }
                        }
                    });
                });
                LOG.info("current allObserveTopics = {}, allBlacklistTp = {}, zkObservers = {}",
                    this.allObserveTopics, this.allBlacklistTp, this.zkObservers);
            } catch (Exception e) {
                LOG.error("observerScheduler occur error, zkObservers = {}", this.zkObservers, e);
            }
        }, 60, 180, TimeUnit.SECONDS);
    }

    public void update(Properties observeInput){
        if (observeInput.containsKey("zkHost") && observeInput.containsKey("topics")){
            if (this.allObserveTopics.containsKey(observeInput.getProperty("zkHost"))){
                this.allObserveTopics.get(observeInput.getProperty("zkHost")).addAll((List<String>)observeInput.get("topics"));
            }else {
                this.allObserveTopics.put(observeInput.getProperty("zkHost"), new HashSet<>((List<String>)observeInput.get("topics")));
            }
            if (!this.allProps.containsKey(observeInput.getProperty("zkHost"))){
                this.allProps.put(observeInput.getProperty("zkHost"), observeInput);
            }
        }
    }

    public Map<Tuple2<String, String>, List<String>> getAllBlacklistTp(){
        this.zkObservers.forEach((k, v) -> this.allBlacklistTp.putAll(v.getObserveTps()));
        return this.allBlacklistTp;
    }

    public void initZkObserver(Properties observeInput){
        if (observeInput.containsKey("zkHost") && observeInput.containsKey("blacklistPath")){
            String zkHost = observeInput.getProperty("zkHost");
            String blacklistPath = observeInput.getProperty("blacklistPath");
            try {
                ZkObserver zkObserver = new ZkObserver(zkHost, blacklistPath);
                this.zkObservers.put(zkHost, zkObserver);
                LOG.info("Thread {}, create zkHost {} 's zkObserver = {}"
                    , Thread.currentThread(), zkHost, zkObserver);
            } catch (Exception e) {
                LOG.error("init zk client error, zkHost = {}", zkHost, e);
            }
        }
    }
}
