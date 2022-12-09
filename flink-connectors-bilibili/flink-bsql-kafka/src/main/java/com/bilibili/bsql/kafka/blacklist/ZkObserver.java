package com.bilibili.bsql.kafka.blacklist;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.zookeeper.*;

import java.io.Serializable;
import java.util.*;

/**
 * @Author weizefeng
 * @Date 2022/2/27 11:43
 **/
@Slf4j
@Data
public class ZkObserver implements Watcher, Serializable {

    private transient ZooKeeper zkClient;

    private String zkHost;

    private String blacklistPath;

    private String blacklistManualPath;

    private List<String> topics;

    private Map<Tuple2<String, String>, List<String>> observeTps;

    public ZkObserver(String zkHost, String blacklistPath) {
        this.zkHost = zkHost;
        this.blacklistPath = blacklistPath + "_auto";
        this.blacklistManualPath = blacklistPath + "_manual";
        this.topics = new ArrayList<>();
        this.observeTps = new HashMap<>();
        initZkClient(this.zkHost);
    }

    private void initZkClient(String zkHost) {
        try {
            this.zkClient = new ZooKeeper(zkHost, 10 * 60 * 1000, this);

            if (null == zkClient.exists(this.blacklistPath, this)) {
                zkClient.create(this.blacklistPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            if (null == zkClient.exists(this.blacklistManualPath, this)) {
                zkClient.create(this.blacklistManualPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            for (String topic : this.topics) {
                this.addWatcher(zkHost, topic);
            }
        } catch (Exception e) {
            log.error("init zkClient error, topics = {}, zkHost = {}, blacklistPath = {}", this.topics, this.zkHost, this.blacklistPath, e);
        }
    }

    protected void addWatcher(String zkHost, String topic) throws Exception{
        if (null == zkClient.exists(this.blacklistPath + "/" + topic, this)){
            zkClient.create(this.blacklistPath + "/" + topic, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("create persistent node for topic {} in zk {}", topic, zkHost);
        }

        if (null == zkClient.exists(this.blacklistManualPath + "/" + topic, this)){
            zkClient.create(this.blacklistManualPath + "/" + topic, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("create persistent node for topic {} in zk {}", topic, zkHost);
        }
        if (!this.topics.contains(topic)){
            this.topics.add(topic);
        }
        this.observeBlackList(topic);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged ||
            event.getType() == Watcher.Event.EventType.NodeChildrenChanged ||
            event.getType() == Watcher.Event.EventType.NodeDeleted) {
            log.info("receive event, event = {}, eventPath = {}, current allBlacklistTps = {}"
                , event.toString(), event.getPath(), this.observeTps);
            this.topics.forEach(this::observeBlackList);
        }
    }

    public void observeBlackList(String topic) {
        try {
            // 如果手动注册路径下有tp则直接以这个tp为准
            List<String> manualBlacklistTps
                = zkClient.getChildren(this.blacklistManualPath + "/" + topic, true, null);
            if (null != manualBlacklistTps && manualBlacklistTps.size() > 0){
                this.observeTps.put(new Tuple2<>(this.zkHost, topic), manualBlacklistTps);
                log.info("receive new manual blacklist tps = {}, topic = {}, observeTps = {}"
                    , manualBlacklistTps, topic, this.observeTps);
                return;
            }
        } catch (KeeperException | InterruptedException e) {
            log.error("Failed to get Children of {}", this.blacklistManualPath, e);
        }

        try {
            List<String> blacklistTps = zkClient.getChildren(this.blacklistPath + "/" + topic, true, null);
            this.observeTps.put(new Tuple2<>(this.zkHost, topic), blacklistTps);
            log.info("receive new blacklist tps = {}, topic = {}, observeTps = {}", blacklistTps, topic, this.observeTps);
        } catch (KeeperException | InterruptedException e) {
            log.error("Failed to get Children of {}", this.blacklistPath, e);
        }
    }

    @Override
    public String toString() {
        return "ZkObserver{" +
            "zkClient=" + zkClient +
            ", zkHost='" + zkHost + '\'' +
            ", blacklistPath='" + blacklistPath + '\'' +
            ", blacklistManualPath='" + blacklistManualPath + '\'' +
            ", topics=" + topics +
            ", observeTp=" + observeTps +
            '}';
    }
}
