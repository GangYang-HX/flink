import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.util.Objects;

/**
 * @author zhuzhengjun
 * @date 2020/11/10 5:30 下午
 */
public class TaishanSinkTest {

    @Test
    public void testTaishanSink() throws InterruptedException {


        String[] args = new String[0];
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

		tEnv.executeSql("CREATE TABLE Source (\n" +
			"  -- declare the schema of the table\n" +
			"  `name` STRING,\n" +
			"  va STRING,\n" +
			"  `num` INT,\n" +
			"  `xtime` as proctime()\n" +
				/*"  `xtime` as now_ts(),\n"+
				"  WATERMARK FOR xtime as xtime - INTERVAL '10.000' SECOND\n"+*/
			") WITH (\n" +
			"  -- declare the external system to connect to\n" +
			"  'connector' = 'bsql-datagen',\n" +
			"  'rows-per-second' = '1'\n" +
			")");
        tEnv.executeSql("CREATE TABLE sink_ (\n" +
                "  -- declare the schema of the table\n" +
                "  name varchar,\n" +
                "  va varchar,\n" +
                "  PRIMARY KEY(name) NOT ENFORCED\n" +

                ") WITH (\n" +
                "  -- declare the external system to connect to\n" +
                "  'connector' = 'bsql-taishan',\n" +
                "  'cluster' = 'common',\n" +
                "  'zone' = 'sz001',\n" +
                "  'ttl' = '3000',\n" +
                "  'table' = 'bigdata_test',\n" +
                "  'password' = '/4TutGjXeX/HxDI+6enmzQ==(已加密)',\n" +
                "  'batchSize' = '1000',\n" +
                "  'parallelism' = '10'\n" +
                ")");

//        tEnv.executeSql("CREATE TABLE MyUserTable (\n" +
//                "  -- declare the schema of the table\n" +
//                "  `name` STRING,\n" +
////			"  `num` INT,\n" +
////			"  `xtime` TimeStamp,\n" +
//                "  `family1` ROW<num INT, xtime TimeStamp(3)>,\n" +
//                "  primary key(name) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "  -- declare the external system to connect to\n" +
//                "  'connector' = 'bsql-hbase',\n" +
//                "  'version' = 'family1.num',\n" +
//                "  'tableName' = 'aaa',\n" +
//                "  'zookeeperQuorum' = 'bbb',\n" +
//                "  'zookeeperParent' = 'ccc',\n" +
//                "  'url' = '127.0.0.1'\n" +
//
//                ")");

        StatementSet stateSet =tEnv.createStatementSet();
        stateSet.addInsertSql("insert into sink_ " +
                "SELECT name as name, va as va FROM Source");

        stateSet.execute("aaa");

//        while (true) {
//            Thread.sleep(1000L);
//        }
    }
}
