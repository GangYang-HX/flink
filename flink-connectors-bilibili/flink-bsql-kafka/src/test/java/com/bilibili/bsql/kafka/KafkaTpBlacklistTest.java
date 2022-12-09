package com.bilibili.bsql.kafka;

import com.bilibili.bsql.kafka.blacklist.ProducerBlacklistTpUtils;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.blacklist.ConsumerBlacklistTpUtils;
import org.apache.flink.streaming.connectors.kafka.blacklist.ReportLagAccumulator;
import org.apache.flink.streaming.connectors.kafka.blacklist.ZkReporter;
import org.apache.flink.streaming.connectors.kafka.partitioner.RandomRetryPartitioner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;

/**
 * @Description
 * @Author weizefeng
 * @Date 2022/3/27 15:03
 **/
@Slf4j
public class KafkaTpBlacklistTest {

    @Test
    public void sinkTest() throws Exception {
        Duration duration = Duration.create("30 s");
        System.err.println(duration);


        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        Configuration configuration = new Configuration();
        configuration.setString(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, "30 sec");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(1);

        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else {
            System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
                "where planner (it is either flink or blink, and the default is blink) indicates whether the " +
                "example uses flink planner or blink planner.");
            return;
        }

        tEnv.executeSql("CREATE TABLE source_test (\n" +
            "  id BIGINT,\n" +
            "  topic_name string metadata from 'topic', \n" +
            " `partition_id` INT METADATA FROM 'partition',\n" +
            "  mock_time TIMESTAMP(3),\n" +
            "  `offset` BIGINT METADATA,\n" +
            "  `timestamp` TIMESTAMP(3) METADATA,\n" +
            " `timestamp-type` string METADATA,\n" +
            "  computed_column AS PROCTIME(),\n" +
            "  `headers` MAP<STRING, BYTES> METADATA,\n" +
            "  WATERMARK FOR mock_time AS mock_time - INTERVAL '5' SECOND\n" +
            ") WITH(\n" +
            "  'connector' = 'bsql-kafka10',\n" +
            "  'offsetReset' = 'earliest',\n" +
            "  'topic' = 'kafka_proxy_test_topic_900000',\n" +
            "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
            "  'bsql-delimit.delimiterKey' = ',',\n" +
            "  'blacklist.enable'='true',\n" +
            "  'blacklist.zk.host'='localhost:2181',\n" +
            "  'blacklist.lag.times'= '8'" +
            ")");


        tEnv.executeSql("CREATE TABLE sink_test (\n" +
            "  id BIGINT,\n" +
            "  topic_name string ,\n" +
            " `partition_id` INT ,\n" +
            "  mock_time TIMESTAMP(3),\n" +
            "  `offset` BIGINT ,\n" +
            "  `timestamp` TIMESTAMP(3),\n" +
            " `timestamp-type` string,\n" +
            "  computed_column TIMESTAMP(3),\n" +
            "  `headers` MAP<STRING, BYTES>\n" +
            ") WITH (\n" +
            "  'connector' = 'bsql-kafka10',\n" +
            "  'topic' = 'kafka_proxy_test_topic_900000',\n" +
            "   'partitionStrategy' = 'BUCKET',\n" +
            "  'bootstrapServers' = '10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092',\n" +
            "  'bsql-delimit.delimiterKey' = ',',\n" +
            "  'blacklist.enable'='true',\n" +
            "  'blacklist.zk.host'='localhost:2181'" +
            ")");


        /*tEnv.toAppendStream(tEnv.sqlQuery("select * from source_test"), Row.class).print();
        env.execute();*/

        StatementSet stateSet = tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO sink_test " +
            "SELECT " +
            "a.id," +
            "a.topic_name," +
            "a.partition_id," +
            "a.mock_time," +
            "a.`offset`," +
            "a.`timestamp`," +
            "a.`timestamp-type`," +
            "a.computed_column," +
            "a.`headers`" +
            " FROM source_test as a");
        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void blacklistPartitionerTest(){
        RandomRetryPartitioner<Object> partitioner = new RandomRetryPartitioner<Object>() {
            @Override
            public int getPartition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                return 0;
            }
        };
        partitioner.initBlacklistPartitioner( null);
        boolean blacklistTp = partitioner.isBlacklistTp(0, "");
        log.info("blacklistTps = {}", blacklistTp);
    }

    @Test
    public void blacklistPartitionerTest2(){
        RandomRetryPartitioner<Object> partitioner = new RandomRetryPartitioner<Object>() {
            @Override
            public int getPartition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                return 0;
            }
        };
        partitioner.initBlacklistPartitioner( "");
        boolean blacklistTp = partitioner.isBlacklistTp(0, "123");
        log.info("blacklistTps = {}", blacklistTp);
    }

    @Test
    public void filterTpTest(){
        RandomRetryPartitioner<Object> partitioner = new RandomRetryPartitioner<Object>() {
            @Override
            public int getPartition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                return 0;
            }
        };
        int[] partitions = partitioner.filterTp(new int[]{1, 2, 3, 4}, "123");
        log.info("partitions = {}", partitions);
    }

    @Test
    public void filterTpTest1(){
        RandomRetryPartitioner<Object> partitioner = new RandomRetryPartitioner<Object>() {
            @Override
            public int getPartition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                return 0;
            }
        };
        partitioner.initBlacklistPartitioner("localhost:2181");
        ConcurrentHashMap<Tuple2<String, String>, List<String>> map = new ConcurrentHashMap<>();
        map.put(new Tuple2<>("localhost:2181", "test-topic"), Arrays.asList("1", "2"));
        partitioner.setAllBlacklistTps(map, Maps.newHashMap());
        int[] filterTps = partitioner.filterTp(new int[]{1, 2, 3, 4}, "test-topic");
        log.info("filterTps = {}", filterTps);
    }

    @Test
    public void judgeReleaseFailedPartitionTest(){
        RandomRetryPartitioner<Object> partitioner = new RandomRetryPartitioner<Object>() {
            @Override
            public int getPartition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                return 0;
            }
        };
        partitioner.initBlacklistPartitioner("localhost:2181");
        partitioner.judgeReleaseFailedPartition(new int[]{1,2,3,4}, "test-topic");
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
            zkReporter = new ZkReporter(
                "localhost:2181",
                "/blacklist",
                Arrays.asList("test-topic"),
                "test3",
                100,
                2,
                1
            );
        } catch (Exception e) {
            log.error("exception = {}", e);
        }
        HashMap<Tuple2<String, Integer>, Long> tpLags = new HashMap<>();
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 8), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 7), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 6), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 5), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 4), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 3), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 2), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 1), 3222040L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 0), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 9), 17103697L);


        ZkReporter finalZkReporter = zkReporter;
        new Thread(new Runnable() {
            @Override
            public void run() {
                finalZkReporter.registerBlackListTp("lancer_wzf_test_2", tpLags);
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        TimeUnit.MINUTES.sleep(60);
    }

    @Test
    public void testZkUnable () throws Exception{
        HashMap<TopicPartition, Long> tpLags = new HashMap<>();
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 8), 0L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 7), 0L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 6), 0L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 5), 0L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 4), 0L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 3), 0L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 2), 0L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 1), 3222040L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 0), 0L);
        tpLags.put(new TopicPartition("lancer_wzf_test_2", 9), 17103697L);



        ReportLagAccumulator reportLagAccumulator = new ReportLagAccumulator();
        Properties properties = new Properties();
        properties.put("blacklist.zk.host","localhost:2181");
        properties.put("blacklist.zk.root.path", "/blacklist");
        properties.put("blacklist.lag.threshold", "100");
        properties.put("blacklist.kick.threshold", "100");
        properties.put("blacklist.lag.times", "1");
        reportLagAccumulator.update(ConsumerBlacklistTpUtils.generateInputProps(properties, Arrays.asList("lancer_wzf_test_2"), "", tpLags));
        TimeUnit.MINUTES.sleep(60);
    }

    @Test
    public void testGetMedian(){
        HashMap<Tuple2<String, Integer>, Long> tpLags = new HashMap<>();
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 8), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 7), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 6), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 5), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 4), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 3), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 2), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 1), 3222040L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 0), 0L);
        tpLags.put(new Tuple2<String, Integer>("lancer_wzf_test_2", 9), 17103697L);
        Collection<Long> values = tpLags.values();
        double median = new ZkReporter().getMedian(tpLags
            .values()
            .stream()
            .map(lag-> Double.valueOf(String.valueOf(lag)))
            .collect(Collectors.toList()));
    }

    @Test
    public void testCheckBlacklistTp(){
        HashMap<Tuple2<String, String>, List<String>> blacklist = new HashMap<>();
        blacklist.put(new Tuple2<String, String>("localhost:2181", "lancer_wzf_test_2"), Arrays.asList("1", "2"));

        HashMap<String, Integer> map = new HashMap<>();
        map.put("lancer_wzf_test_2", 2);
        ProducerBlacklistTpUtils.checkBlacklistTp(blacklist, map);
    }

    @Test
    public void testNullBlacklistTps(){
        RandomRetryPartitioner<Object> partitioner = new RandomRetryPartitioner<Object>() {
            @Override
            public int getPartition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                return 0;
            }
        };
        HashMap<String, int[]> map = Maps.newHashMap();
        map.put("test-topic1", new int[]{1, 2, 3, 4});
        partitioner.setAllBlacklistTps(Maps.newHashMap(), map);

        System.out.println(Arrays.stream(partitioner.getFilteredPartitions("test-topic2", new int[]{1, 2, 3, 4})).boxed().map(i -> i).collect(Collectors.toList()));
    }

    @Test
    public void testRegisterBlacklistTp() throws InterruptedException {

        ReportLagAccumulator reportLagAccumulator = new ReportLagAccumulator();

        for (int i = 0; i < 2; i++) {
            Thread.sleep(10000);
            reportLagAccumulator.update(TpBlacklistUtils.generateProps(TpBlacklistUtils.generateTpLags()));

            Thread.sleep(10000);
            reportLagAccumulator.update(TpBlacklistUtils.generateProps(TpBlacklistUtils.generateTpLags2()));
        }
    }

    static class TpBlacklistUtils {

        public static Properties generateProps(Map<Tuple3<String, String, Integer>, Long> tpLags){
            String zkHost = "localhost:2181";
            String blacklistPath = "/blacklist";
            List<String> topics = new ArrayList<String>(){
                {
                    add("lancer_wzf_test_1");
                    add("lancer_wzf_test_2");
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
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 0), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 1), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 2), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 3), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 4), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 5), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 6), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 7), 3222040L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 8), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 9), 17103697L);

            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 0), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 1), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 2), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 3), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 4), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 5), 2222222L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 6), 3333333L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 7), 3222040L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 8), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 9), 17103697L);
            return tpLags;
        }

        public static Map<Tuple3<String, String, Integer>, Long> generateTpLags2() {
            HashMap<Tuple3<String, String, Integer>, Long> tpLags = new HashMap<>();
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 0), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 1), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 2), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 3), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 4), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 5), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 6), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 7), 3222040L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 8), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_1", 9), 0L);

            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 0), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 1), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 2), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 3), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 4), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 5), 2222222L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 6), 3333333L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 7), 3222040L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 8), 0L);
            tpLags.put(new Tuple3<String, String, Integer>("localhost:2181", "lancer_wzf_test_2", 9), 17103697L);
            return tpLags;
        }
    }
}
