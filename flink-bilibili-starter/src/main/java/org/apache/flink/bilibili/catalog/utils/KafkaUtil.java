package org.apache.flink.bilibili.catalog.utils;

import org.apache.flink.bilibili.catalog.KeeperConnection;
import org.apache.flink.bilibili.sql.SqlTree;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhangyang
 * @Date:2022/6/6
 * @Time:01:05
 */
public class KafkaUtil {

    //kafka表匹配正则表达式
    private static final String  KAFKA_TABLE_PATTERN_STR = "\\s`?Kafka_[A-Za-z0-9_-]+`?\\.`?([A-Za-z0-9_-]+)`?(\\.`?([A-Za-z0-9_-]+)`?)?(\\.`?([A-Za-z0-9_-]+)`?)?\\s";
    public static final Pattern KAFKA_TABLE_PATTERN = Pattern.compile(KAFKA_TABLE_PATTERN_STR);

    public static String convert(String multiSql){
        return kafkaTableConvert(multiSql);
    }
    private static String kafkaTableConvert(String sql) {
        sql = sql + " ";
        //解析并解决kafka topic中带"."的情况，以及两段氏解析
        Matcher matcher = KAFKA_TABLE_PATTERN.matcher(sql);
        while (matcher.find()) {
            String[] split = matcher.group(0).replaceAll("`", "").trim().split("\\.");
            switch (split.length) {
                case 2:
                    sql = sql.replaceAll(matcher.group(0), " Kafka_1.default_database." + split[1] + " ");
                    break;
                case 3:
                    boolean kafkaTableIsExist = KeeperConnection.getInstance().tableExist(split[0], "", split[1] + "." + split[2]);
                    if (kafkaTableIsExist) {
                        sql = sql.replaceAll(matcher.group(0), " Kafka_1.default_database.`" + split[1] + "." + split[2] + "` ");
                    }
                    break;
                case 4:
                    sql = sql.replaceAll(matcher.group(0), " Kafka_1." + split[1] + ".`" + split[2] + "." + split[3] + "` ");						break;
            }
        }
        return sql;
    }
}
