package com.bilibili.bsql.hdfs.internal;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhangyang
 * @Date:2020/5/19
 * @Time:10:47 AM
 */
public class FileFetcher<OUT> implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(FileFetcher.class);
    private final SourceFunction.SourceContext<OUT> sourceContext;
    private final List<FilePartition> filePartitions;
    private final FileSourceProperties sourceProperties;
    private final DeserializationSchema<OUT> deserializationSchema;
    private BlockingQueue<String> handover;
    private Consumer consumer;
    private Producer producer;
    private Map<FilePartition, ContinuousReader> readerMap;
    private final Integer index;
    private volatile AtomicBoolean producerEnd = new AtomicBoolean(false);

    public FileFetcher(Integer index, SourceFunction.SourceContext<OUT> sourceContext, DeserializationSchema<OUT> deserializationSchema,
                       List<FilePartition> filePartitions, FileSourceProperties sourceProperties) {
        this.index = index;
        this.sourceContext = sourceContext;
        this.deserializationSchema = deserializationSchema;
        this.filePartitions = filePartitions;
        this.sourceProperties = sourceProperties;
    }

    public void open() throws Exception {
        readerMap = new HashMap<>(filePartitions.size());
        for (FilePartition filePartition : filePartitions) {
            ContinuousReader reader = new ContinuousReader(sourceProperties, filePartition);
            reader.open();
            readerMap.put(filePartition, reader);
        }
        LOG.info("readerMap init success:{}", readerMap.keySet());
        handover = new ArrayBlockingQueue<>(sourceProperties.getFetchSize() * 5);
        consumer = new Consumer();
        producer = new Producer();
    }

    public List<FilePartition> snapshot() {
        List<FilePartition> copy = new ArrayList<>(readerMap.size());
        for (FilePartition partition : readerMap.keySet()) {
            FilePartition copyPartition = new FilePartition(new ArrayList<>(partition.getFileList()));
            copyPartition.setFileIndex(readerMap.get(partition).getActiveFileIndex());
            copyPartition.setFileOffset(readerMap.get(partition).getActiveFileOffset());
            copy.add(copyPartition);
        }
        return copy;
    }

    public void run() {
        consumer.start();
        producer.start();
        try {
            producer.join();
            consumer.join();
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    public void close() {
        long start = System.currentTimeMillis();
        producer.interrupt();
        consumer.interrupt();
        long cost = System.currentTimeMillis() - start;
        LOG.info("close success,cost: {}ms", cost);
    }

    public boolean reachEnd() {
        return producerEnd.get();
    }

    class Producer extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    boolean allEmpty = true;
                    for (ContinuousReader reader : readerMap.values()) {
                        try {
                            List<String> fetch = reader.readLines(sourceProperties.getFetchSize());
                            if (CollectionUtils.isNotEmpty(fetch)) {
                                allEmpty = false;
                                for (String it : fetch) {
                                    handover.put(it);
                                }
                            }
                        } catch (IOException e) {
                            allEmpty = false;
                            LOG.error("read data error", e);
                        }
                    }
                    if (allEmpty) {
                        producerEnd.set(true);
                        LOG.info("partition:{} reach end,file:{}", index, JSON.toJSONString(filePartitions));
                        break;
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    class Consumer extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    String item = handover.take();
                    OUT row = deserializationSchema.deserialize(item.getBytes());
                    if (row == null) {
                        LOG.error("row is null:{}", item);
                    } else {
                        sourceContext.collect(row);
                    }
                } catch (InterruptedException | IOException e) {
                    LOG.info("consumer is interrupted");
                    break;
                }
            }
        }
    }

}
