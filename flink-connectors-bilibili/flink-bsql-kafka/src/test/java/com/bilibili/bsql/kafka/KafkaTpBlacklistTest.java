package com.bilibili.bsql.kafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.blacklist.ConsumerBlacklistTpUtils;
import org.apache.flink.connector.kafka.blacklist.ProducerBlacklistTpUtils;
import org.apache.flink.connector.kafka.blacklist.ReportLagAccumulator;
import org.apache.flink.connector.kafka.blacklist.ZkReporter;

import com.bilibili.bsql.kafka.partitioner.RandomRetryPartitioner;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** KafkaTpBlacklistTest. */
@Slf4j
public class KafkaTpBlacklistTest {

    @Test
    public void blacklistPartitionerTest() {
        RandomRetryPartitioner<Object> partitioner =
                new RandomRetryPartitioner<Object>() {
                    @Override
                    public int getPartition(
                            Object record,
                            byte[] key,
                            byte[] value,
                            String targetTopic,
                            int[] partitions) {
                        return 0;
                    }
                };
        partitioner.initBlacklistPartitioner(getProps(), null, null);
        boolean blacklistTp = partitioner.isBlacklistTp(0, "");
        log.info("blacklistTps = {}", blacklistTp);
    }

    @Test
    public void blacklistPartitionerTest2() {
        RandomRetryPartitioner<Object> partitioner =
                new RandomRetryPartitioner<Object>() {
                    @Override
                    public int getPartition(
                            Object record,
                            byte[] key,
                            byte[] value,
                            String targetTopic,
                            int[] partitions) {
                        return 0;
                    }
                };
        partitioner.initBlacklistPartitioner(getProps(), null, null);
        boolean blacklistTp = partitioner.isBlacklistTp(0, "123");
        log.info("blacklistTps = {}", blacklistTp);
    }

    @Test
    public void filterTpTest() {
        RandomRetryPartitioner<Object> partitioner =
                new RandomRetryPartitioner<Object>() {
                    @Override
                    public int getPartition(
                            Object record,
                            byte[] key,
                            byte[] value,
                            String targetTopic,
                            int[] partitions) {
                        return 0;
                    }
                };
        int[] partitions = partitioner.filterTp(new int[] {1, 2, 3, 4}, "sink_topic");
        log.info("partitions = {}", partitions);
    }

    @Test
    public void filterTpTest1() {
        RandomRetryPartitioner<Object> partitioner =
                new RandomRetryPartitioner<Object>() {
                    @Override
                    public int getPartition(
                            Object record,
                            byte[] key,
                            byte[] value,
                            String targetTopic,
                            int[] partitions) {
                        return 0;
                    }
                };
        partitioner.initBlacklistPartitioner(getProps());
        ConcurrentHashMap<Tuple2<String, String>, List<String>> map = new ConcurrentHashMap<>();
        map.put(new Tuple2<>("localhost:2181", "sink_topic"), Arrays.asList("1", "2"));
        partitioner.setAllBlacklistTps(map, Maps.newHashMap());
        int[] filterTps = partitioner.filterTp(new int[] {1, 2, 3, 4}, "sink_topic");
        log.info("filterTps = {}", filterTps);
    }

    @Test
    public void judgeReleaseFailedPartitionTest() {
        RandomRetryPartitioner<Object> partitioner =
                new RandomRetryPartitioner<Object>() {
                    @Override
                    public int getPartition(
                            Object record,
                            byte[] key,
                            byte[] value,
                            String targetTopic,
                            int[] partitions) {
                        return 0;
                    }
                };
        partitioner.initBlacklistPartitioner(getProps());
        partitioner.judgeReleaseFailedPartition(new int[] {1, 2, 3, 4}, "sink-topic");
    }

    @Test
    public void zkWatcherTest() throws InterruptedException {
        String zkHost = "localhost:2181";
        Properties inputProps = new Properties();
        inputProps.put("zkHost", zkHost);
        inputProps.put("blacklistPath", "/test-blackTp");
        inputProps.put("topics", Arrays.asList("test-topic1", "test-topic2"));
        inputProps.put("jobId", "");
        Tuple3<String, String, Integer> tuple3 = new Tuple3<>("localhost:2181", "test-topic1", 1);
        Map<Tuple3<String, String, Integer>, Long> tpLags = new HashMap<>();
        tpLags.put(tuple3, 222222L);
        inputProps.put("tpLags", tpLags);

        ReportLagAccumulator lagAccumulator = new ReportLagAccumulator();
        for (int i = 0; i < 10; i++) {
            lagAccumulator.update(inputProps);
            log.info("update success");
            Thread.sleep(30000);
        }
    }

    @Test
    public void testCalculateBlacklistTp() throws Exception {
        ZkReporter zkReporter = null;
        try {
            zkReporter =
                    new ZkReporter(
                            "localhost:2181",
                            "/blacklist",
                            Arrays.asList("source_topic"),
                            "test3",
                            100,
                            2,
                            1);
        } catch (Exception e) {
            log.error("exception = {}", e);
        }
        HashMap<Tuple2<String, Integer>, Long> tpLags = new HashMap<>();
        tpLags.put(new Tuple2<String, Integer>("source_topic", 5), 0L);
        tpLags.put(new Tuple2<String, Integer>("source_topic", 4), 0L);
        tpLags.put(new Tuple2<String, Integer>("source_topic", 3), 0L);
        tpLags.put(new Tuple2<String, Integer>("source_topic", 2), 0L);
        tpLags.put(new Tuple2<String, Integer>("source_topic", 1), 3222040L);
        tpLags.put(new Tuple2<String, Integer>("source_topic", 0), 0L);
        tpLags.put(new Tuple2<String, Integer>("source_topic", 6), 17103697L);

        ZkReporter finalZkReporter = zkReporter;
        finalZkReporter.registerBlackListTp("source_topic", tpLags);
    }

    @Test
    public void testZkUnable() throws Exception {
        HashMap<TopicPartition, Long> tpLags = new HashMap<>();
        tpLags.put(new TopicPartition("sink_topic", 8), 0L);
        tpLags.put(new TopicPartition("sink_topic", 7), 0L);
        tpLags.put(new TopicPartition("sink_topic", 6), 0L);
        tpLags.put(new TopicPartition("sink_topic", 5), 0L);
        tpLags.put(new TopicPartition("sink_topic", 4), 0L);
        tpLags.put(new TopicPartition("sink_topic", 3), 0L);
        tpLags.put(new TopicPartition("sink_topic", 2), 0L);
        tpLags.put(new TopicPartition("sink_topic", 1), 3222040L);
        tpLags.put(new TopicPartition("sink_topic", 0), 0L);
        tpLags.put(new TopicPartition("sink_topic", 9), 17103697L);

        ReportLagAccumulator reportLagAccumulator = new ReportLagAccumulator();
        Properties properties = new Properties();
        properties.put("blacklist.zk.host", "localhost:2181");
        properties.put("blacklist.zk.root.path", "/blacklist");
        properties.put("blacklist.lag.threshold", "100");
        properties.put("blacklist.kick.threshold", "100");
        properties.put("blacklist.lag.times", "1");
        reportLagAccumulator.update(
                ConsumerBlacklistTpUtils.generateInputProps(
                        toConfiguration(properties), tpLags, 1));
    }

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }

    @Test
    public void testGetMedian() {
        HashMap<Tuple2<String, Integer>, Long> tpLags = new HashMap<>();
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 0), 314665326L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 1), 190966047L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 2), 274898943L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 3), 223007999L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 4), 298315617L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 5), 202863601L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 6), 336738588L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 7), 231851432L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 8), 299684953L);
        tpLags.put(new Tuple2<String, Integer>("r_ods.tpc_blacklist_diversion3", 9), 230705011L);
        Collection<Long> values = tpLags.values();
        double median =
                new ZkReporter()
                        .getMedian(
                                tpLags.values().stream()
                                        .map(lag -> Double.valueOf(String.valueOf(lag)))
                                        .collect(Collectors.toList()));
        System.out.println(median);

        double averageLag =
                tpLags.values().stream().mapToDouble(lag -> lag).average().getAsDouble();

        Map<Tuple2<String, Integer>, Long> kickTps =
                tpLags.entrySet().stream()
                        .filter(
                                tpLag ->
                                        tpLag.getValue() > Math.min(averageLag, median) * 10
                                                && tpLag.getValue() > 50000)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        System.out.println(kickTps);
        System.out.println(averageLag);
    }

    @Test
    public void testCheckBlacklistTp() {
        HashMap<Tuple2<String, String>, List<String>> blacklist = new HashMap<>();
        blacklist.put(
                new Tuple2<String, String>("localhost:2181", "sink_topic"),
                Arrays.asList("1", "2"));

        HashMap<String, Integer> map = new HashMap<>();
        map.put("sink_topic", 1);
        ProducerBlacklistTpUtils.checkBlacklistTp(blacklist, map);
    }

    @Test
    public void testNullBlacklistTps() {
        RandomRetryPartitioner<Object> partitioner =
                new RandomRetryPartitioner<Object>() {
                    @Override
                    public int getPartition(
                            Object record,
                            byte[] key,
                            byte[] value,
                            String targetTopic,
                            int[] partitions) {
                        return 0;
                    }
                };
        HashMap<String, int[]> map = Maps.newHashMap();
        map.put("sink_topic", new int[] {1, 2, 3, 4});
        partitioner.setAllBlacklistTps(Maps.newHashMap(), map);

        System.out.println(
                Arrays.stream(
                                partitioner.getFilteredPartitions(
                                        "sink_topic", new int[] {1, 2, 3, 4}))
                        .boxed()
                        .map(i -> i)
                        .collect(Collectors.toList()));
    }

    @Test
    public void testRegisterBlacklistTp() throws InterruptedException {

        ReportLagAccumulator reportLagAccumulator = new ReportLagAccumulator();

        for (int i = 0; i < 2; i++) {
            reportLagAccumulator.update(
                    TpBlacklistUtils.generateProps(TpBlacklistUtils.generateTpLags()));
            Thread.sleep(5000);
            reportLagAccumulator.update(
                    TpBlacklistUtils.generateProps(TpBlacklistUtils.generateTpLags2()));
            Thread.sleep(5000);
        }
    }

    static Properties getProps() {
        Properties props = new Properties();
        props.put("blacklist.zk.host", "localhost:2181");
        return props;
    }

    static class TpBlacklistUtils {

        public static Properties generateProps(Map<Tuple3<String, String, Integer>, Long> tpLags) {
            String zkHost = "localhost:2181";
            String blacklistPath = "/blacklist";
            List<String> topics =
                    new ArrayList<String>() {
                        {
                            add("source_topic");
                            add("sink_topic");
                        }
                    };
            String jobId = "aaa";

            Properties props = new Properties();
            props.put("zkHost", zkHost);
            props.put("blacklistPath", blacklistPath);
            props.put("topics", topics);
            props.put("jobId", jobId);
            props.put("tpLags", tpLags);
            props.put("lagThreshold", "1000");
            props.put("kickThreshold", "3");
            props.put("lagTimes", "10");
            return props;
        }

        public static Map<Tuple3<String, String, Integer>, Long> generateTpLags() {
            HashMap<Tuple3<String, String, Integer>, Long> tpLags = new HashMap<>();
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 0), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 1), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 2), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 4), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 5), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 3),
                    3222040L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 6),
                    17103697L);

            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 0), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 1), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 2), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 5),
                    2222222L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 3),
                    3333333L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 4),
                    3222040L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 6),
                    17103697L);
            return tpLags;
        }

        public static Map<Tuple3<String, String, Integer>, Long> generateTpLags2() {
            HashMap<Tuple3<String, String, Integer>, Long> tpLags = new HashMap<>();
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 0), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 1), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 2), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 3), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 4), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 5), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "source_topic", 6),
                    3222040L);

            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 0), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 1), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 2),
                    2222222L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 3),
                    3333333L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 4),
                    3222040L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 5), 0L);
            tpLags.put(
                    new Tuple3<String, String, Integer>("localhost:2181", "sink_topic", 6),
                    17103697L);
            return tpLags;
        }
    }
}
