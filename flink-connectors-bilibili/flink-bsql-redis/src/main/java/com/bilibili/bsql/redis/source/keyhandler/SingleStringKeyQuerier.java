/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.redis.source.keyhandler;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

/**
 *
 * @author zhouxiaogang
 * @version $Id: SingleStringKeyQuerier.java, v 0.1 2020-12-03 16:44
zhouxiaogang Exp $$
 */
public class SingleStringKeyQuerier<V> implements KeyGenerator<String, V> {

    private RedisStringAsyncCommands asyncCommands;

    public SingleStringKeyQuerier(RedisStringAsyncCommands commands) {

        this.asyncCommands = commands;
    }

    @Override
    public String createCacheKey(Object... inputs) {
        return inputs[0].toString();
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
    public String createKeyFromInput(Object... inputs) {
        return inputs[0].toString();
    }

    @Override
    public RedisFuture<V> queryRedis(String singleKey) {
        return asyncCommands.get(singleKey);
    }
}