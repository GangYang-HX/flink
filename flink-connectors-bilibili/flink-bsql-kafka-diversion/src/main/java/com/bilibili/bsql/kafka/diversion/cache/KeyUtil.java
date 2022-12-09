package com.bilibili.bsql.kafka.diversion.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/** KeyUtil. */
public class KeyUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KeyUtil.class);
    public static final String UNCACHE_KEY = "nocache";

    public static String transformKey(Object... keys) {
        try {
            StringBuilder encodeKey = new StringBuilder();
            Arrays.stream(keys)
                    .forEach(
                            key ->
                                    encodeKey.append(
                                            Base64.getEncoder()
                                                    .encodeToString(
                                                            String.valueOf(key)
                                                                    .getBytes(
                                                                            StandardCharsets
                                                                                    .UTF_8))));
            return encodeKey.toString();
        } catch (Exception e) {
            LOG.warn("transofm key [{}] meet error: {}", Arrays.toString(keys), e);
            return UNCACHE_KEY;
        }
    }
}
