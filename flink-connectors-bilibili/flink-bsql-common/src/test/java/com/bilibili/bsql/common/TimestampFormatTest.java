package com.bilibili.bsql.common;

import com.bilibili.bsql.common.format.converter.CustomRowConverter;
import org.apache.flink.table.data.TimestampData;
import org.junit.Test;

import java.time.format.DateTimeFormatter;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className TimestampFormatTest.java
 * @description This is the description of TimestampFormatTest.java
 * @createTime 2020-12-09 15:09:00
 */
public class TimestampFormatTest {

    @Test
    public void timestampTest() throws Exception {

        /*yyyy-MM-dd'T'HH:mm:ssz
        yyyy-MM-dd'T'HH:mm:ss.SSSz
        yyyy-MM-dd'T'HH:mm:ss*/
        String[] timePattern = {
                "2020-12-09 14:37:00.0", // yyyy-MM-dd HH:mm:ss
                "2020-12-09T14:37:00.156Z", // yyyy-MM-dd'T'HH:mm:ssz
                "2017-11-27T03:16:03Z",// yyyy-MM-dd'T'HH:mm:ss.SSSz
                "2020-12-09T14:37:00" //yyyy-MM-dd'T'HH:mm:ss
        };

        System.out.println("2020-12-09T14:37:00".length());

        for (String time : timePattern) {
            TimestampData timestampData = new CustomRowConverter(null).convertToTimestamp(time);
            System.out.println(timestampData);
        }
    }
}
