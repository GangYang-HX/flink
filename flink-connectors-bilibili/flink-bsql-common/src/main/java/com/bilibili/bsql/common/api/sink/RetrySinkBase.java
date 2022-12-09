package com.bilibili.bsql.common.api.sink;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * Base class for retry sink.
 *
 * @param <OUT>
 */
public abstract class RetrySinkBase<OUT> implements RetrySink<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(RetrySinkBase.class);

    private LinkedBlockingQueue<OUT> retryQueue;

    private RetrySender<OUT> retrySender;

    private int subtaskId;

    protected transient ExecutorService retryPool;

    public RetrySinkBase(
            int subtaskId, LinkedBlockingQueue<OUT> retryQueue, RetrySender<OUT> retrySender) {
        this.retryQueue = retryQueue;
        this.retrySender = retrySender;
        this.subtaskId = subtaskId;
        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("retry-sink-thread" + this.subtaskId + "-%d")
                        .build();
        this.retryPool = Executors.newSingleThreadExecutor(namedThreadFactory);
    }

    @Override
    public void start() {
        this.retryPool.execute(
                () -> {
                    while (true) {
                        try {
                            OUT record = this.retryQueue.take();
                            this.retrySender.sendRecord(record);
                        } catch (InterruptedException e) {
                            LOG.error("kafka sink retry take from queue error", e);
                        }
                    }
                });
    }

    @Override
    public void addRetryRecord(OUT record) throws InterruptedException {
        this.retryQueue.put(record);
    }

    @Override
    public Integer getPendingRecordsSize() {
        return this.retryQueue.size();
    }

    @Override
    public void close() throws Exception {
        this.retryPool.shutdown();
        this.retrySender.close();
    }
}
