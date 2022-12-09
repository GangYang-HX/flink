package com.bilibili.bsql.redis.source;

import org.apache.flink.table.data.RowData;

import com.bilibili.bsql.redis.source.keyhandler.KeyGenerator;
import io.lettuce.core.RedisFuture;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/** @version TimeOutFuture.java */
public class TimeOutFuture {

    private long time;
    private Object queryKey;
    private CompletableFuture<Collection<RowData>> resultFuture;
    private RedisLookupFunction.Outputer redisRowDataOutputer;
    private KeyGenerator keyGenerator;
    private RedisFuture futureForValue;
    private int retry;
    private long timeout;
    private Object exception;

    private ScheduledFuture<?> timeOutScheduledFuture;

    public TimeOutFuture(
            KeyGenerator keyGenerator,
            RedisFuture futureForValue,
            RedisLookupFunction.Outputer redisRowDataOutputer,
            CompletableFuture<Collection<RowData>> resultFuture,
            Object queryKey,
            long time,
            int retry) {
        this.queryKey = queryKey;
        this.time = System.currentTimeMillis();
        this.timeout = System.currentTimeMillis() + time;
        this.resultFuture = resultFuture;
        this.redisRowDataOutputer = redisRowDataOutputer;
        this.keyGenerator = keyGenerator;
        this.futureForValue = futureForValue;
        this.retry = retry;
        this.exception = null;
    }

    public ScheduledFuture<?> getTimeOutScheduledFuture() {
        return timeOutScheduledFuture;
    }

    public void setTimeOutScheduledFuture(ScheduledFuture<?> timeOutScheduledFuture) {
        this.timeOutScheduledFuture = timeOutScheduledFuture;
    }

    public long getTime() {
        return time;
    }

    public Object getQueryKey() {
        return queryKey;
    }

    public CompletableFuture<Collection<RowData>> getResultFuture() {
        return resultFuture;
    }

    public RedisLookupFunction.Outputer getRedisRowDataOutputer() {
        return redisRowDataOutputer;
    }

    public KeyGenerator getKeyGenerator() {
        return keyGenerator;
    }

    public RedisFuture getFutureForValue() {
        return futureForValue;
    }

    public int getRetry() {
        return retry;
    }

    public Object getException() {
        return exception;
    }

    public TimeOutFuture setException(Object exception) {
        this.exception = exception;
        return this;
    }

    @Override
    public String toString() {
        return "TimeOutFuture{"
                + "time="
                + time
                + ", queryKey="
                + queryKey
                + ", resultFuture="
                + resultFuture
                + ", redisRowDataOutputer="
                + redisRowDataOutputer
                + ", keyGenerator="
                + keyGenerator
                + ", futureForValue="
                + futureForValue
                + ", retry="
                + retry
                + ", timeout="
                + timeout
                + '}';
    }
}
