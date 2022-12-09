package com.bilibili.bsql.redis.source;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** @version TimeOutHandle.java */
public class TimeOutHandle {
    private static final Logger LOG = LoggerFactory.getLogger(TimeOutHandle.class);

    private boolean hasError;
    private int maxRetry;
    private ScheduledThreadPoolExecutor timerService;
    private long timeout;
    private Throwable error;

    public TimeOutHandle(int maxRetry, long timeout) {
        timerService = new ScheduledThreadPoolExecutor(1);
        this.hasError = false;
        this.maxRetry = maxRetry;
        this.timeout = timeout;
        LOG.info("TimeOutHandle init timeout:{}  maxRetry:{}", timeout, maxRetry);
    }

    public boolean isHasError() {
        return hasError;
    }

    public Throwable getError() {
        return error;
    }

    public void shutDown() {
        if (timerService != null) {
            timerService.shutdown();
        }
    }

    public void scheduleHandle(TimeOutFuture timeOutFuture) {
        if (!timeOutFuture.getFutureForValue().isDone() || timeOutFuture.getException() != null) {
            // 重试逻辑
            if (!timeOutFuture.getFutureForValue().isCancelled()) {
                timeOutFuture.getFutureForValue().cancel(true);
            }

            LOG.info(
                    "redis query timeout. key :{}  retry:{}",
                    timeOutFuture.getQueryKey().toString(),
                    timeOutFuture.getRetry() + 1);

            RedisFuture redisFuture =
                    timeOutFuture.getKeyGenerator().queryRedis(timeOutFuture.getQueryKey());
            TimeOutFuture retryTimeOutFuture =
                    new TimeOutFuture(
                            timeOutFuture.getKeyGenerator(),
                            redisFuture,
                            timeOutFuture.getRedisRowDataOutputer(),
                            timeOutFuture.getResultFuture(),
                            timeOutFuture.getQueryKey(),
                            timeout,
                            timeOutFuture.getRetry() + 1);

            redisFuture.thenAccept(
                    (Object redisValue) -> {
                        try {
                            timeOutFuture
                                    .getRedisRowDataOutputer()
                                    .handleRedisQuery(
                                            retryTimeOutFuture.getQueryKey(),
                                            retryTimeOutFuture.getResultFuture(),
                                            retryTimeOutFuture.getTime() - timeout,
                                            redisValue);
                            retryTimeOutFuture.getTimeOutScheduledFuture().cancel(true);
                        } catch (Throwable t) {
                            LOG.error("exception happened when redis join : ", t);
                            retryTimeOutFuture.getResultFuture().completeExceptionally(t);
                            throw t;
                        }
                    });

            redisFuture.exceptionally(
                    o ->
                            handleException(
                                    o, retryTimeOutFuture, timeOutFuture.getQueryKey().toString()));

            addTimeOutHandle(retryTimeOutFuture);
        }
    }

    public Object handleException(Object o, TimeOutFuture timeOutFuture, String key) {
        if (o instanceof RedisCommandExecutionException) {
            LOG.error("redis command query error, key {} : ", key);
            timeOutFuture
                    .getResultFuture()
                    .completeExceptionally((RedisCommandExecutionException) o);
            throw (RedisCommandExecutionException) o;
        } else if (o instanceof CompletionException) {
            // disconnect or killed by TaskManager
            timeOutFuture.setException(o);
            LOG.info(
                    "redis asyn return CompletionException, input-key: {}, will retry again!", key);
        } else if (o instanceof CancellationException) {
            // be cancelled, will not handle exception
            // 1). query success
            // 2). triggered by timeout
            return null;
        } else if (o instanceof Exception) {
            // unknow exception, will not handle exception
            Exception ignore = (Exception) o;
            ignore.printStackTrace();
            LOG.info(
                    "redis asyn return unhandle exception: {}, input-key: {}",
                    o.getClass().getName(),
                    key);
        }
        return o;
    }

    public void addTimeOutHandle(TimeOutFuture timeOutFuture) {
        if (timeOutFuture.getRetry() >= maxRetry) {
            Throwable throwable =
                    new RuntimeException(
                            String.format(
                                    "redis query error. key :%s  retry:%s",
                                    timeOutFuture.getQueryKey().toString(),
                                    timeOutFuture.getRetry()));
            timeOutFuture.getResultFuture().completeExceptionally(throwable);
            LOG.error(
                    "redis query error. key :{}  retry:{}",
                    timeOutFuture.getQueryKey().toString(),
                    timeOutFuture.getRetry());
            hasError = true;
            error = throwable;
            return;
        }

        ScheduledFuture<?> scheduledFuture =
                timerService.schedule(
                        new Runnable() {
                            @Override
                            public void run() {
                                scheduleHandle(timeOutFuture);
                            }
                        },
                        timeout,
                        TimeUnit.MILLISECONDS);

        timeOutFuture.setTimeOutScheduledFuture(scheduledFuture);
    }
}
