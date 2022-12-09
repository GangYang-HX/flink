package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author weizefeng
 * @Date 2022/3/7 20:43
 **/
public abstract class BlacklistPartitioner<T> extends FlinkKafkaPartitioner<T>{

    private final static Logger LOG = LoggerFactory.getLogger(BlacklistPartitioner.class);
    /**
     *  < zkHost, topic>, blacklistTps</>
     */
    private Map<Tuple2<String, String>, List<String>> allBlacklistTps = new ConcurrentHashMap<>();
    private String zkHost;
    private boolean enableBlacklist = false;
    private Map<String, int[]> filteredPartitions = new ConcurrentHashMap<>();

    public BlacklistPartitioner() {
    }

    public void initBlacklistPartitioner(String zkHost){
        this.zkHost = zkHost;
        this.enableBlacklist = true;
    }

    public Map<Tuple2<String, String>, List<String>> allBlacklistTps() {
        return allBlacklistTps;
    }

    public void setAllBlacklistTps(Map<Tuple2<String, String>, List<String>> allBlacklistTps, Map<String, int[]> allTps) {
        this.allBlacklistTps = allBlacklistTps;
        allTps.forEach((topic, partitions) -> {
            this.filteredPartitions.put(topic, this.filterTp(partitions, topic));
        });
        LOG.info("current filteredPartitions = {}", this.filteredPartitions.keySet()
            .stream()
            .collect(Collectors.toMap(
                tp-> tp,
                tp -> Arrays.stream(this.filteredPartitions.get(tp)).boxed().collect(Collectors.toList())
        )));
    }

    public int[] getFilteredPartitions(String targetTopic, int[] oldPartitions) {
        return this.filteredPartitions.getOrDefault(targetTopic, oldPartitions);
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

    /**
     * remove blacklist tp in old partition
     */
    public int[] filterTp(int[] oldPartition, String targetTopic){
        // 1. 先将该zkHost下的topic的黑名单tp过滤出来
        List<Integer> blacklistTps = this.allBlacklistTps
            .getOrDefault(new Tuple2<String, String>(this.zkHost, targetTopic), Collections.emptyList())
            .stream()
            .map(Integer::parseInt)
            .collect(Collectors.toList());
        if (blacklistTps.size() == 0){
            return oldPartition;
        }
        List<Integer> tempPartition = Arrays.stream(oldPartition).boxed().collect(Collectors.toList());
        tempPartition.removeIf(blacklistTps::contains);
        int[] filteredPartition = tempPartition.stream().mapToInt(i -> i).toArray();
        // this judgment guarantees data integrity in some unexpected case
        return (filteredPartition.length < oldPartition.length / 2 || filteredPartition.length > oldPartition.length)
            ? oldPartition : filteredPartition;
    }

    // only for test
    public boolean isBlacklistTp(int partition, String targetTopic){
        if (!enableBlacklist || null == this.allBlacklistTps || null == this.zkHost){
            return false;
        }
        List<Integer> blacklistTps = this.allBlacklistTps
            .getOrDefault(new Tuple2<>(this.zkHost, targetTopic), Collections.emptyList())
            .stream()
            .map(Integer::parseInt)
            .collect(Collectors.toList());
        return blacklistTps.contains(partition);
    }
}
