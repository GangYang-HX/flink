import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
import scala.concurrent.duration.Duration;

public class TaishanSideJoinTest {
    @Test
    public void testSideJoin() throws InterruptedException {
        Duration duration = Duration.create("30 s");
        System.err.println(duration);

        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(1);

        StreamTableEnvironment tEnv;
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        tEnv = StreamTableEnvironment.create(env, settings);
        Configuration timeoutConf = new Configuration();
        timeoutConf.setString("table.exec.async-lookup.timeout", "100 ms");
        tEnv.getConfig().addConfiguration(timeoutConf);
        tEnv.executeSql("CREATE TABLE source_test (\n" +
                "  id1 BIGINT,\n" +
                "  id2 BIGINT\n" +
                ") WITH(\n" +
                "  'connector' = 'bsql-datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.id1.min' = '1',\n" +
                "  'fields.id1.max' = '2',\n" +
                "  'fields.id2.min' = '3',\n" +
                "  'fields.id2.max' = '4'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE taishan_side (\n" +
                "  key VARCHAR,\n" +
                "  val ARRAY<BYTES>,\n" +
                "  PRIMARY KEY (key) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'cluster' = 'common',\n" +
                "  'zone' = 'sh001',\n" +
                "  'table' = 'mmz_taishan_test',\n" +
                "  'connector' = 'bsql-taishan',\n" +
                //"  'cache' = 'LRU',\n" +
                "  'delimitKey' = ',',\n" +
                "  'timeout' = '50',\n" +
                "  'password' = 'mmz_taishan_test'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_test (\n" +
                "  key1 BIGINT,\n" +
                "  key2 BIGINT,\n" +
                "  val ARRAY<BYTES>\n" +
                ") WITH (\n" +
                "  'connector' = 'bsql-log'\n" +
                ")");

        StatementSet stateSet =tEnv.createStatementSet();
        stateSet.addInsertSql("INSERT INTO sink_test " +
                "SELECT a.id1,a.id2, val FROM source_test a left JOIN " +
                "taishan_side FOR SYSTEM_TIME AS OF now() AS b ON " +
                //"concat('key', cast(a.id1 as VARCHAR(1))) = b.key");
                "concat(concat(concat('key', cast(a.id1 as VARCHAR(1))), ','), concat('key', cast(a.id2 as VARCHAR(1)))) = b.key");


        stateSet.execute("aaa");

        while (true) {
            Thread.sleep(1000L);
        }
    }
}
