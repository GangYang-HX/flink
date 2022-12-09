package com.bilibili.bsql.kafka.diversion;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.bilibili.bsql.kafka.diversion.cache.GuavaCacheFactory;
import com.google.common.cache.Cache;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/** CacheTest. */
public class CacheTest {
    @Test
    public void testCache() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Cache<String, String> cache = new GuavaCacheFactory<String, String>().getCache(0);
        while (true) {
            System.out.println(cache.get("1", () -> getValue("1")));
            Thread.sleep(1000L);
        }
    }

    private String getValue(String key) {
        System.out.println(
                "get "
                        + key
                        + " at "
                        + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        return "v1";
    }
}
