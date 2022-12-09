/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.producer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.bilibili.bsql.kafka.ability.metadata.write.WritableMetadata;
import com.bilibili.bsql.kafka.blacklist.ProducerBlacklistTpUtils;
import com.bilibili.bsql.kafka.blacklist.ObserveAggFunc;
import com.bilibili.bsql.kafka.util.KafkaMetadataParser;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.blacklist.ConsumerBlacklistTpUtils;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.RandomRetryPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.trace.Trace;
import org.apache.flink.types.RowKind;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import com.bilibili.bsql.kafka.sink.HashKeySerializationSchemaWrapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Headers;

import javax.annotation.Nullable;

/**
 *
 * @author zhouxiaogang
 * @version $Id: RetryKafkaProducer.java, v 0.1 2020-12-17 15:49
zhouxiaogang Exp $$
 */
@Slf4j
public class RetryKafkaProducer extends BsqlKafkaProducer {

    private transient LinkedBlockingQueue<RetryDataWithCount> retryQueue;

	protected transient ExecutorService retryPool;

	protected transient ScheduledThreadPoolExecutor resetPool;

	protected HashKeySerializationSchemaWrapper wrapper;

	protected AtomicLong retryingPendingRecords;

	protected RandomRetryPartitioner retryPartitioner;

	protected Long MaxSnapshotRetry;

	protected Long RetryRecords;

	protected Long OverSizeRecordIsIgnored;

	private Boolean isOpenKafkaOverSize = false;

	private Trace trace;

    private GlobalAggregateManager aggregateManager;

    private ScheduledThreadPoolExecutor aggScheduler;

    public RetryKafkaProducer(String topicId,
							  HashKeySerializationSchemaWrapper serializationSchema,
							  Properties producerConfig,
							  @Nullable FlinkKafkaPartitioner<RowData> customPartitioner,
							  Integer timeoutRetryTimes) {
        super(
                topicId,
                serializationSchema,
                producerConfig,
                customPartitioner
        );
        if(customPartitioner instanceof RandomRetryPartitioner){
			this.retryPartitioner = (RandomRetryPartitioner) customPartitioner;
			this.retryPartitioner.setKafkaFailRetry(true);
		}else {
        	throw new RuntimeException("Partitioner don't support retry,please check partitioner and retry config");
		}
        this.flushOnCheckpoint = false;
		this.wrapper = serializationSchema;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        retryQueue = new LinkedBlockingQueue<RetryDataWithCount>();
        retryingPendingRecords = new AtomicLong();
        MaxSnapshotRetry = 0L;
        RetryRecords = 0L;
        OverSizeRecordIsIgnored = 0L;
        getRuntimeContext().getMetricGroup().gauge(
                "kafkaRetryingRecord",
                (Gauge<Long>) () -> retryingPendingRecords.get()
        );

        getRuntimeContext().getMetricGroup().gauge(
                "kafkaMaxSnapshotRetry",
                (Gauge<Long>) () -> MaxSnapshotRetry
        );

        getRuntimeContext().getMetricGroup().gauge(
                "kafkaRetryRecords",
                (Gauge<Long>) () -> RetryRecords
        );

        getRuntimeContext().getMetricGroup().gauge(
                "kafkaOverSizeRecordIsIgnored",
                (Gauge<Long>) () ->OverSizeRecordIsIgnored
        );

        retryPool = Executors.newSingleThreadExecutor();
        resetPool = new ScheduledThreadPoolExecutor(1);

        resetPool.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                retryPartitioner.resetPartition();
            }
        }, 1, 1, TimeUnit.MINUTES);
        retryPool.submit(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        RetryDataWithCount retryDataWithCount = retryQueue.take();
                        int failedTimes = retryDataWithCount.failedTimes;
                        ProducerRecord<byte[], byte[]> producerRecord = retryDataWithCount.record;
                        byte[] serializedKey = producerRecord.key();
                        byte[] serializedValue = producerRecord.value();
                        Long timestamp = producerRecord.timestamp();
                        Headers headers = producerRecord.headers();

                        String targetTopic = producerRecord.topic();
                        int[] partitions = topicPartitionsMap.get(targetTopic);
                        if (null == partitions) {
                            partitions = getPartitionsByTopic(targetTopic, producer);
                            topicPartitionsMap.put(targetTopic, partitions);
                        }

                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                                targetTopic,
                                retryPartitioner.partition(
                                        new GenericRowData(0),
                                        serializedKey,
                                        serializedValue,
                                        targetTopic,
                                        partitions
                                ),
                                timestamp, serializedKey,
                                serializedValue, headers
                        );
                        producer.send(record, new RetryCallBack(record, failedTimes));

                    } catch (InterruptedException e) {
                        log.error("kafka sink retry take from queue error", e);
                    }  catch (Exception e) {
                        log.error("kafka sink retry error", e);
                    }
                }
            }
        });

        this.trace = Trace.createTraceClient(
            getRuntimeContext().getUserCodeClassLoader(),
            this.producerConfig.get(TRACE_KIND).toString(),
            this.producerConfig.get(CUSTOM_TRACE_CLASS).toString(),
            this.producerConfig.get(TRACE_ID).toString()
        );

		if (Boolean.parseBoolean(this.producerConfig.getProperty(ProducerBlacklistTpUtils.BLACKLIST_ENABLE))
			&& this.producerConfig.containsKey(ProducerBlacklistTpUtils.BLACKLIST_ZK_HOST)){
			startAggScheduler(topicPartitionsMap);
		}

        this.isOpenKafkaOverSize = (Boolean) this.producerConfig.get(IS_KAFKA_OVER_SIZE_RECORD_IGNORE);
        log.info("isOpenKafkaOverSize is {}", isOpenKafkaOverSize);
        log.info("properties = {}, trace = {}", this.producerConfig, this.trace);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        Long snapshotTriggerTime = ctx.getCheckpointTimestamp();
        flush();
        while (true) {
            if ((System.currentTimeMillis() - snapshotTriggerTime) > 8*60*1000) {
                log.error("snapshot {} last for too much time {} to {}", ctx.getCheckpointId(),
                        snapshotTriggerTime, System.currentTimeMillis());
                return;
            }

            if (this.retryingPendingRecords.get() == 0) {
                log.info("all the pending retry record released for ck {}", ctx.getCheckpointId());
                MaxSnapshotRetry = 0L;
                return;
            }

            Thread.sleep(200L);
            flush();
            MaxSnapshotRetry++;
        }
    }

    @Override
    public void invoke(RowData value, SinkFunction.Context context) throws Exception {
        while (retryingPendingRecords.get() > 5_000_000) {
            log.info("kafka retry sink has too mush retrying records, slowing the process down");
            Thread.sleep(5000L);
        }

        long start = System.nanoTime();
		if (value.getRowKind() == RowKind.UPDATE_BEFORE || value.getRowKind() == RowKind.DELETE) {
			return;
		}
        try {
			HashKeySerializationSchemaWrapper.kafkaRecord wrapperRecord = wrapper.generateRecord(value);


			byte[] serializedKey = wrapperRecord.key;
			byte[] serializedValue = wrapperRecord.value;
			String targetTopic = schema.getTargetTopic(value);
			if (null == targetTopic) {
				targetTopic = defaultTopicId;
			}

            ProducerRecord<byte[], byte[]> record;
            int[] partitions = topicPartitionsMap.get(targetTopic);
            if (null == partitions) {
                partitions = getPartitionsByTopic(targetTopic, producer);
                topicPartitionsMap.put(targetTopic, partitions);
            }

            record = new ProducerRecord<>(
                    targetTopic,
                    retryPartitioner.partition(value, serializedKey, serializedValue, targetTopic, partitions),
				   	wrapper.readMetaData(value, WritableMetadata.TIMESTAMP), serializedKey,
                    serializedValue,wrapper.readMetaData(value, WritableMetadata.HEADERS)
            );

			if (flushOnCheckpoint) {
				synchronized (pendingRecordsLock) {
					pendingRecords++;
				}
			}
			this.producer.send(record, new FirstCallBack(record, 0));
			sinkMetricsGroup.rtWriteRecord(start);
			sinkMetricsGroup.writeSuccessSize(getMessageSize(serializedKey, serializedValue));
		} catch (Exception e) {
			sinkMetricsGroup.writeFailureRecord();
			throw new Exception("Failed to write kafka record.", e);
		}
		sinkMetricsGroup.writeSuccessRecord();
	}

    protected void acknowledgeRetryMessage() {
        retryingPendingRecords.decrementAndGet();
    }

    private void acknowledgeMessage() {
        if (this.flushOnCheckpoint) {
            synchronized(this.pendingRecordsLock) {
                --this.pendingRecords;
                if (this.pendingRecords == 0L) {
                    this.pendingRecordsLock.notifyAll();
                }
            }
        }
    }

    public void startAggScheduler(Map<String, int[]> allTps){
        this.retryPartitioner.initBlacklistPartitioner(
            this.producerConfig.getProperty(ProducerBlacklistTpUtils.BLACKLIST_ZK_HOST));
        aggregateManager = ((StreamingRuntimeContext) getRuntimeContext()).getGlobalAggregateManager();
        aggScheduler = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("RetryKafkaProducer-agg-scheduler-" + getRuntimeContext().getIndexOfThisSubtask()).build());
        aggScheduler.scheduleWithFixedDelay(()->{
			try {
				this.updateAgg(allTps);
			} catch (Exception e) {
				log.error("updateAgg error, ", e);
			}
		}, 60, 90, TimeUnit.SECONDS);
        ProducerBlacklistTpUtils.printProps("RetryKafkaProducer", this.producerConfig);
    }

    public void updateAgg(Map<String, int[]> allTps){
        if (allTps.size() == 0){
            return;
        }
        Map<Tuple2<String, String>, List<String>> result;
        try {
            Properties inputProps = ProducerBlacklistTpUtils.generateInputProps(
                this.producerConfig,
                new ArrayList<>(allTps.keySet()),
                ConsumerBlacklistTpUtils.getJobId((StreamingRuntimeContext) getRuntimeContext())
            );

            Properties output = this.aggregateManager.updateGlobalAggregate(
                ObserveAggFunc.NAME,
                inputProps,
                new ObserveAggFunc()
            );
            result = (Map<Tuple2<String, String>, List<String>>) output.get("allBlacklistTps");
            this.retryPartitioner.setAllBlacklistTps(ProducerBlacklistTpUtils.checkBlacklistTp(
                result,
                ProducerBlacklistTpUtils.getKickThresholdMap(allTps, Integer.parseInt(this.producerConfig.getProperty(ProducerBlacklistTpUtils.BLACKLIST_KICK_THRESHOLD)))),
				allTps
            );
        } catch (Exception e) {
            log.error("execute observe agg error, clear allBlacklistTps", e);
            this.retryPartitioner.setAllBlacklistTps(Maps.newHashMap(), allTps);
        }
    }

    public class RetryCallBack implements Callback {
        public ProducerRecord<byte[], byte[]> record;
        int failedTimes;
        public long ctime;
        public long requestTime;
        public long updateTime;

        public RetryCallBack(ProducerRecord<byte[], byte[]> record, int failedTimes) {
            this.record = record;
            this.failedTimes = failedTimes;
            long current = System.currentTimeMillis();
            Map<String, String> kafkaHeaders = KafkaMetadataParser.parseKafkaHeader(record.headers());
            this.ctime = Long.parseLong(kafkaHeaders.getOrDefault(CREATE_TIME, String.valueOf(current)));
            this.requestTime = Long.parseLong(kafkaHeaders.getOrDefault(REQUEST_TIME, String.valueOf(current)));
            this.updateTime = Long.parseLong(kafkaHeaders.getOrDefault(UPDATE_TIME, String.valueOf(current)));
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                try{
                    /*
                     * 保存失败partition必须早于重试记录
                     *
                     * */
                    RetryKafkaProducer.this.retryPartitioner.addFailedPartition(record.partition());
                    failedTimes++;
                    RetryKafkaProducer.this.retryQueue.put(
                            new RetryDataWithCount(record, failedTimes)
                    );
                } catch (InterruptedException e) {
                    log.error("kafka retry put into queue error", e);
                }

                trace.traceError(
                    this.ctime,
                    1,
                    "internal.error",
                    "",
                    exception.getMessage(),
                    "KAFKA");
            } else {
                if (failedTimes != 0) {
                    /*
                     * 当重试的记录写成功了，acknowledge pending retry
                     * 正常发送成功的不会走到这里
                     * */
                    acknowledgeRetryMessage();
                }

                long current = System.currentTimeMillis();
                trace.traceEvent(
                    this.ctime,
                    1,
                    "batch.output.send",
                    "",
                    (int) (current - this.updateTime),
                    "KAFKA");

                trace.traceEvent(
                    this.requestTime,
                    1,
                    "output.send",
                    "",
                    (int) (current - this.requestTime),
                    "KAFKA");

            }
            /*
            * 不会 acknowledgeMessage, 因为是已经失败过的
            * */
        }
    }

    public class FirstCallBack implements Callback {
        public ProducerRecord<byte[], byte[]> record;
        public int failedTimes;
        public long ctime;
        public long requestTime;
        public long updateTime;


        public FirstCallBack(ProducerRecord<byte[], byte[]> record, int failedTimes) {
            this.record = record;
            this.failedTimes = failedTimes;
            long current = System.currentTimeMillis();
            Map<String, String> kafkaHeaders = KafkaMetadataParser.parseKafkaHeader(record.headers());
            this.ctime = Long.parseLong(kafkaHeaders.getOrDefault(CREATE_TIME, String.valueOf(current)));
            this.requestTime = Long.parseLong(kafkaHeaders.getOrDefault(REQUEST_TIME, String.valueOf(current)));
            this.updateTime = Long.parseLong(kafkaHeaders.getOrDefault(UPDATE_TIME, String.valueOf(current)));
        }

        protected void handleException(Exception exception) {
			if (exception instanceof RetriableException) {
				log.warn("write to kafka occured exception: {}", exception.getMessage());
			} else if (isOpenKafkaOverSize && exception instanceof RecordTooLargeException) {
				OverSizeRecordIsIgnored++;
				log.info("kafka record size over max field,current size is {},current max.request.size is {},current OverSizeRecordIsIgnored value is {}\n", getMessageSize(record.key(), record.value()), producerConfig.getProperty("max.request.size"), OverSizeRecordIsIgnored);
			} else {
				throw new RuntimeException(exception);
			}
		}

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
            	handleException(exception);
            	/*
            	 * 忽略处理，不再重试
            	 **/
            	if (isOpenKafkaOverSize && exception instanceof RecordTooLargeException){
            		return;
            	}
                try{
                    /*
                     * 保存失败partition必须早于重试记录
                     *
                     * */
                    RetryKafkaProducer.this.retryPartitioner.addFailedPartition(record.partition());

                    /*
                    * 只有第一次失败的需要记录
                    * */
                    retryingPendingRecords.incrementAndGet();
                    failedTimes++;
                    RetryKafkaProducer.this.RetryRecords++;
                    RetryKafkaProducer.this.retryQueue.put(
                            new RetryDataWithCount(record, failedTimes)
                    );

                    trace.traceError(
                        this.ctime,
                        1,
                        "internal.error",
                        "",
                        exception.getMessage(),
                        "KAFKA");

                } catch (InterruptedException e) {
                    log.error("kafka sink put into retry queue error", e);
                }
            } else {
                long current = System.currentTimeMillis();
                trace.traceEvent(
                    this.ctime,
                    1,
                    "batch.output.send",
                    "",
                    (int) (current - this.updateTime),
                    "KAFKA");

                trace.traceEvent(
                    this.requestTime,
                    1,
                    "output.send",
                    "",
                    (int) (current - this.requestTime),
                    "KAFKA");
            }

            acknowledgeMessage();
        }
    }
}
