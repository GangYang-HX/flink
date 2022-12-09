package com.bilibili.bsql.kafka;

import org.apache.flink.table.utils.ThreadLocalCache;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author zhangyang
 * @Date:2019/10/22
 * @Time:11:28 PM
 */
public class SqlDateTimeUtils {

    private static final ThreadLocalCache<String, SimpleDateFormat> FORMATTER_CACHE = new ThreadLocalCache<String, SimpleDateFormat>() {
        @Override
        public SimpleDateFormat getNewInstance(String key) {
            return new SimpleDateFormat(
                    key);
        }
    };
    private final static TimeZone zone = TimeZone.getTimeZone("GMT+8");

    public static String fromUnixtime(long unixtime, String format) {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(zone);
        Date date = new Date(unixtime);
        return formatter.format(date);
    }

    public static Long toUnixtime(String date, String format) throws ParseException {
        SimpleDateFormat formatter = FORMATTER_CACHE.get(format);
        formatter.setTimeZone(zone);
        return formatter.parse(date).getTime();
    }

    public static String dataReformat(String date, String format, String newFormat) throws ParseException {
        SimpleDateFormat oldFormatter = FORMATTER_CACHE.get(format);
        SimpleDateFormat newFormatter = FORMATTER_CACHE.get(newFormat);
        return newFormatter.format(oldFormatter.parse(date));
    }
}
