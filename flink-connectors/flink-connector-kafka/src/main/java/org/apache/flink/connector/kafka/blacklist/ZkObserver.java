package org.apache.flink.connector.kafka.blacklist;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.WatchedEvent;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.Watcher;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.ZooDefs;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** zookeeper node info Observer. */
public class ZkObserver implements Watcher, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ZkObserver.class);

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
                zkClient.create(
                        this.blacklistPath,
                        "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }

            if (null == zkClient.exists(this.blacklistManualPath, this)) {
                zkClient.create(
                        this.blacklistManualPath,
                        "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }

            for (String topic : this.topics) {
                this.addWatcher(zkHost, topic);
            }
        } catch (Exception e) {
            LOG.error(
                    "init zkClient error, topics = {}, zkHost = {}, blacklistPath = {}",
                    this.topics,
                    this.zkHost,
                    this.blacklistPath,
                    e);
        }
    }

    protected void addWatcher(String zkHost, String topic) throws Exception {
        if (null == zkClient.exists(this.blacklistPath + "/" + topic, this)) {
            zkClient.create(
                    this.blacklistPath + "/" + topic,
                    "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            LOG.info("create persistent node for topic {} in zk {}", topic, zkHost);
        }

        if (null == zkClient.exists(this.blacklistManualPath + "/" + topic, this)) {
            zkClient.create(
                    this.blacklistManualPath + "/" + topic,
                    "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            LOG.info("create persistent node for topic {} in zk {}", topic, zkHost);
        }
        if (!this.topics.contains(topic)) {
            this.topics.add(topic);
        }
        this.observeBlackList(topic);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged
                || event.getType() == Event.EventType.NodeChildrenChanged
                || event.getType() == Event.EventType.NodeDeleted) {
            LOG.info(
                    "receive event, event = {}, eventPath = {}, current allBlacklistTps = {}",
                    event.toString(),
                    event.getPath(),
                    this.observeTps);
            this.topics.forEach(this::observeBlackList);
        }
    }

    public void observeBlackList(String topic) {
        try {
            // If there is a tp in the manual registration path, this tp shall prevail
            List<String> manualBlacklistTps =
                    zkClient.getChildren(this.blacklistManualPath + "/" + topic, true, null);
            if (null != manualBlacklistTps && manualBlacklistTps.size() > 0) {
                this.observeTps.put(new Tuple2<>(this.zkHost, topic), manualBlacklistTps);
                LOG.info(
                        "receive new manual blacklist tps = {}, topic = {}, observeTps = {}",
                        manualBlacklistTps,
                        topic,
                        this.observeTps);
                return;
            }
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Failed to get Children of {}", this.blacklistManualPath, e);
        }

        try {
            List<String> blacklistTps =
                    zkClient.getChildren(this.blacklistPath + "/" + topic, true, null);
            this.observeTps.put(new Tuple2<>(this.zkHost, topic), blacklistTps);
            LOG.info(
                    "receive new blacklist tps = {}, topic = {}, observeTps = {}",
                    blacklistTps,
                    topic,
                    this.observeTps);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Failed to get Children of {}", this.blacklistPath, e);
        }
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public Map<Tuple2<String, String>, List<String>> getObserveTps() {
        return observeTps;
    }

    public void setObserveTps(Map<Tuple2<String, String>, List<String>> observeTps) {
        this.observeTps = observeTps;
    }

    @Override
    public String toString() {
        return "ZkObserver{"
                + "zkClient="
                + zkClient
                + ", zkHost='"
                + zkHost
                + '\''
                + ", blacklistPath='"
                + blacklistPath
                + '\''
                + ", blacklistManualPath='"
                + blacklistManualPath
                + '\''
                + ", topics="
                + topics
                + ", observeTp="
                + observeTps
                + '}';
    }
}
