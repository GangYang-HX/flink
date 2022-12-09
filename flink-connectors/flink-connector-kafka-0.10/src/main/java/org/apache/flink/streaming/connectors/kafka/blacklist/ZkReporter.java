package org.apache.flink.streaming.connectors.kafka.blacklist;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author weizefeng
 * @Date 2022/2/27 13:14
 **/
public class ZkReporter implements Watcher, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ZkReporter.class);

    private String zkHost;

    private transient ZooKeeper zkClient;

    private String blacklistPath;

    private List<String> topics;

    private String jobId;

    private long lagThreshold;

    private int lagTimes;

    private int kickThreshold;

    /**
     * key: topic, value: topic's blacklist tp registered by current job.
     */
    private Map<String, List<String>> registerBySelf;

    public ZkReporter() {
    }

    public ZkReporter(
        String zkHost,
        String blacklistPath,
        List<String> topics,
        String jobId,
        long lagThreshold,
        int kickThreshold,
        int lagTimes
    ) {
        this.zkHost = zkHost;
        this.blacklistPath = blacklistPath + "_auto";
        this.topics = topics;
        this.jobId = jobId;
        this.lagThreshold = lagThreshold;
        this.lagTimes = lagTimes;
        this.kickThreshold = kickThreshold;
        this.registerBySelf = new HashMap<>();
        initZkClient(zkHost, this.blacklistPath);
    }

    public List<String> topics() {
        return topics;
    }

    @Override
    public void process(WatchedEvent event) {

    }

    private void initZkClient(String zkHost, String blacklistPath) {
        try {
            this.zkClient = new ZooKeeper(zkHost, 10 * 60 * 1000, this);
            if (null == zkClient.exists(blacklistPath, this)) {
                zkClient.create(blacklistPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            for (String topic : this.topics) {
                createZkNodeForTopic(topic);
            }
            LOG.info("create zk client for topics {} success, zkHost = {}, jobId = {}", this.topics, zkHost, this.jobId);
        } catch (Exception e) {
            LOG.error("init zk client for topics {} error, zkHost = {}, jobId = {}", this.topics, zkHost, this.jobId, e);
        }
    }

    public void createZkNodeForTopic(String topic) throws Exception{
        if (null == zkClient.exists(this.blacklistPath + "/" + topic, this)){
            zkClient.create(this.blacklistPath + "/" + topic, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            if (!this.topics.contains(topic)){
                this.topics.add(topic);
            }
            LOG.info("create persistent node for topic {} in zk {}, jobId = {}", topic, this.zkHost, this.jobId);
        }
        this.registerBySelf.putIfAbsent(topic, Lists.newArrayList());
    }

    public void registerBlackListTp(String targetTopic, Map<Tuple2<String, Integer>, Long> tpLags) {
        List<String> blacklistTps = calculateBlacklistTp(targetTopic, tpLags);
        if (blacklistTps.size() == 0){
            deleteZkNode(targetTopic, blacklistTps);
            return;
        }
        try {
            List<String> registeredTp = zkClient.getChildren(this.blacklistPath + "/" + targetTopic, this);
            // 过滤掉已经注册的节点
            List<String> upToRegisterTp = blacklistTps
                .stream()
                .filter(tp -> !registeredTp.contains(tp))
                .collect(Collectors.toList());

            if (0 == upToRegisterTp.size()){
                LOG.info("high lagTps already registered, topic = {}, registeredTp = {}, registerBySelf = {}, {}'s blacklistTps = {}"
                    , targetTopic, registeredTp, this.registerBySelf, targetTopic, blacklistTps);
                deleteZkNode(targetTopic, blacklistTps);
                return;
            }
            for (String tp : upToRegisterTp) {
                this.zkClient.create(
                    this.blacklistPath + "/" + targetTopic + "/" + tp,
                    this.jobId.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL
                );
                this.registerBySelf.getOrDefault(targetTopic, Lists.newArrayList()).add(tp);
            }
            LOG.info("register new blacklistTps success, topic = {}, upToRegisterTp = {}, registeredTp = {}, jobId = {}, current blacklistTps = {}"
                , targetTopic, upToRegisterTp, registeredTp, this.jobId, blacklistTps);
            deleteZkNode(targetTopic, blacklistTps);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("register new blacklistTp failed, topic = {}, jobId = {}, current blacklistTps = {}"
                , targetTopic, this.jobId, blacklistTps, e);
        }
    }

    private void deleteZkNode(String topic, List<String> blacklistTps){
        if (this.registerBySelf.getOrDefault(topic, Collections.emptyList()).size() == 0){
            return;
        }
        List<String> tryToDeleteTpNode = this.registerBySelf
            .getOrDefault(topic, Collections.emptyList())
            .stream()
            .filter(tp -> !blacklistTps.contains(tp))
            .collect(Collectors.toList());
        if (tryToDeleteTpNode.size() > 0) {
            LOG.info("try to delete zk node topic = {}, tryToDeleteTpNode = {}", topic, tryToDeleteTpNode);
        }
        tryToDeleteTpNode.forEach(tp->{
            try {
                this.zkClient.delete(blacklistPath + "/" + topic + "/" + tp, -1);
                this.registerBySelf.getOrDefault(topic, Lists.newArrayList()).remove(tp);
                LOG.info("delete tp Node success, topic = {}, deletedTp = {}, jobId = {}, registerBySelf = {}"
                    , topic, tp, this.jobId, this.registerBySelf);
            } catch (InterruptedException | KeeperException e) {
                LOG.error("delete tp Node error, topic = {}, deletedTp = {}, jobId = {}, registerBySelf = {}"
                    , topic, tp, this.jobId, this.registerBySelf, e);
            }
        });
        if (tryToDeleteTpNode.size() > 0) {
            LOG.info("delete Zk node completed, tryToDeleteTpNode = {}, registerBySelf = {}, {}'s blacklistTps = {}"
                , tryToDeleteTpNode, this.registerBySelf, topic, blacklistTps);
        }
    }

    protected List<String> calculateBlacklistTp(String targetTopic, Map<Tuple2<String, Integer>, Long> tpLags){
        // filter targetTopic's lag data.
        Map<Tuple2<String, Integer>, Long> filterTp = tpLags.entrySet()
            .stream()
            .filter(tpLag -> tpLag.getKey().f0.equals(targetTopic))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ));

        if (filterTp.size() == 0){
            LOG.info("topic = {} 's filterTp.size = 0, tpLags = {}", targetTopic, tpLags);
            return new ArrayList<>();
        }

        double averageLag = filterTp.values()
            .stream()
            .mapToDouble(lag -> lag)
            .average()
            .getAsDouble();

        double median = getMedian(filterTp
            .values()
            .stream()
            .map(lag-> Double.valueOf(String.valueOf(lag)))
            .collect(Collectors.toList())
        );

        LOG.info("filterTps = {}, averageLag = {}, median = {}", filterTp, averageLag, median);
        Map<Tuple2<String, Integer>, Long> kickTps = filterTp.entrySet()
            .stream()
            .filter(tpLag -> tpLag.getValue() > Math.min(averageLag, median) * lagTimes && tpLag.getValue() > lagThreshold)
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ));

        List<String> kickTpList = kickTps.entrySet()
            .stream()
            .sorted(Map.Entry.<Tuple2<String, Integer>, Long>comparingByValue().reversed())
            .map(kickTp -> kickTp.getKey().f1.toString())
            .collect(Collectors.toList());

        int maxKickNum = this.kickThreshold == -1 ? Math.max((int) (filterTp.size() * 0.1), 1) : this.kickThreshold;
        if (kickTpList.size() > maxKickNum){
            kickTpList = kickTpList.subList(0, maxKickNum);
        }
        if (kickTpList.size() == 0){
            LOG.info("no tp to kick, topic = {}, tpLags = {}, kickThreshold = {}", targetTopic, tpLags, maxKickNum);
        }else {
            LOG.info("topic = {} 's kickTpList = {}, kickThreshold = {}", targetTopic, kickTpList, maxKickNum);
        }
        return kickTpList;
    }

    public double getMedian(List<Double> filterTpValues){
        double result = 0;
        Collections.sort(filterTpValues);
        int size = filterTpValues.size();
        if(size % 2 == 1){
            result = filterTpValues.get((size - 1) / 2);
        }else {
            result = (filterTpValues.get(size / 2 - 1) + filterTpValues.get(size / 2) + 0.0) / 2;
        }
        return result;
    }

    @Override
    public String toString() {
        return "ZkReporter{" +
            "zkHost='" + zkHost + '\'' +
            ", zkClient=" + zkClient +
            ", blacklistPath='" + blacklistPath + '\'' +
            ", topics=" + topics +
            ", jobId='" + jobId + '\'' +
            ", lagThreshold=" + lagThreshold +
            ", lagTimes=" + lagTimes +
            ", kickThreshold=" + kickThreshold +
            ", registerBySelf=" + registerBySelf +
            '}';
    }
}
