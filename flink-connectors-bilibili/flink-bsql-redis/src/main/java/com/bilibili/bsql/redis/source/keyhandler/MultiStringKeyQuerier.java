/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis.source.keyhandler;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * @author zhouxiaogang
 * @version $Id: MultiStringKeyQuerier.java, v 0.1 2020-12-03 16:44
zhouxiaogang Exp $$
 */
public class MultiStringKeyQuerier<V> implements KeyGenerator<String[], V> {
    String delimitKey;

    private RedisStringAsyncCommands asyncCommands;

    public MultiStringKeyQuerier(String delimitKey, RedisStringAsyncCommands commands) {
        checkNotNull(delimitKey, "delimitKey should not be null");

        this.delimitKey = delimitKey;
        this.asyncCommands = commands;
    }

    @Override
    public boolean shouldReturnDirectly(Object... inputs) {
        String tmpObject = inputs[0].toString();
        if (tmpObject == null || tmpObject.length() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public String createCacheKey(Object... inputs) {
        throw new UnsupportedOperationException("redis multi query not support the cache for now!!!!! ");
    }

    @Override
    public String[] createKeyFromInput(Object... inputs) {
        /**
         * Make sure empty element be filtered so as not to cause random redis high pressure
         * */
        return StringUtils.splitPreserveAllTokens(inputs[0].toString(), this.delimitKey);
    }

    @Override
    public RedisFuture<V> queryRedis(String[] singleKey) {
        String[] lookupKeys = Arrays.stream(singleKey)
                .filter(a -> !StringUtils.isEmpty(a))
                .toArray(String[]::new);
        return asyncCommands.mget(lookupKeys);
    }
}