package org.apache.flink.bilibili.starter;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.airlift.compress.zstd.ZstdDecompressor;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

public class TestOrcZstd {
    @Before
    public void initKerberos() throws Exception {
        String env = "/Users/yanggang/Downloads/conf";
        System.setProperty("java.security.krb5.conf", env + "/keytabs/krb5.conf");

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(new Path(env, "core-site.xml"));
        conf.addResource(new Path(env, "hdfs-site.xml"));
        conf.addResource(new Path(env, "hive-site.xml"));
        conf.addResource(new Path(env, "yarn-site.xml"));

        String principal = "admin/admin@BILIBILI.CO";
        String keytab = env + "/keytabs/admin.keytab";

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation loginUser;
        if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } else {
            return;
        }
        loginUser = UserGroupInformation.getCurrentUser();
        System.out.println(loginUser);
    }

    @Test
    public void testTwoLevelHivePartitionByPartitionName() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(1000 * 60L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createCatalog = "CREATE CATALOG HIVE_CATALOG WITH (\n"
                + "'type' = 'hive',\n"
                + "'hive-version' = '2.3.6',\n"
                + "'hive-conf-dir' = '/Users/yanggang/Downloads/conf',\n"
                + "'hadoop-conf-dir'='/Users/yanggang/Downloads/conf',\n"
                + "'default-database' = 'yg_test_db'\n"
                + ")\n";
        tableEnv.executeSql(createCatalog);

        String selectSql =
                "SELECT event_id,mid,log_date,log_hour from HIVE_CATALOG.yg_test_db.ods_app_ubt_mall_low"
                        + "/*+ OPTIONS("
                        + "'streaming-source.enable'='true'"
                        + ",'streaming-source.monitor-interval' = '10s'"
                        + ",'streaming-source.partition-order' = 'partition-name'"
                        + ",'streaming-source.consume-start-offset'='log_date=20221013/log_hour=16'"
                        + ")*/";

        tableEnv.executeSql(selectSql).print();
        env.execute();
    }

    @Test
    public void readZstdFile() throws Exception {
        ZstdDecompressor zstdDecompressor = new ZstdDecompressor();

        String inputFile = "/Users/yanggang/Downloads/part-0-6351.orc";
        String outputFile = "/Users/yanggang/Downloads/part-0-6351.txt";

        ByteBuffer inputByte = ByteBuffer.wrap(FileUtils.readFileToByteArray(new File(inputFile)));
        ByteBuffer outByte = ByteBuffer.allocate(19070224 * 10);
//        ByteBuffer outByte = ByteBuffer.wrap(FileUtils.readFileToByteArray(new File(outputFile)));

        zstdDecompressor.decompress(inputByte, outByte);

        System.out.println("read successfully,result to : " + new String(outByte.array()));
    }

    @Test
    public void readOrcFile() throws SerDeException, IOException {
        String fileName = "/Users/yanggang/Downloads/part-99-2886";

        JobConf conf = new JobConf();
        conf.set(HiveConf.ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname, "BI");
        conf.set("hive.io.file.readcolumn.ids", "1");


        Path testFilePath = new Path(fileName);
        Properties p = new Properties();
        OrcSerde serde = new OrcSerde();
        p.setProperty("columns", "labels,click_sum,trackid,mid");
        p.setProperty("columns.types", "string:string:bigint:string");
        serde.initialize(conf, p);
        StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
        InputFormat in = new OrcInputFormat();
        FileInputFormat.setInputPaths(conf, testFilePath.toString());

        InputSplit[] splits = in.getSplits(conf, 1);
        System.out.println("splits.length==" + splits.length);

        RecordReader reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
        Object key = reader.createKey();
        Object value = reader.createValue();
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        long offset = reader.getPos();
        while (reader.next(key, value)) {
            Object url = inspector.getStructFieldData(value, fields.get(0));
            Object word = inspector.getStructFieldData(value, fields.get(3));
            Object freq = inspector.getStructFieldData(value, fields.get(4));
            Object weight = inspector.getStructFieldData(value, fields.get(5));
            offset = reader.getPos();
            System.out.println(url + "|" + word + "|" + freq + "|" + weight);
        }
        reader.close();
    }
}
