package com.bilibili.bsql.databus.fetcher;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bilibili.bsql.databus.tableinfo.BsqlDataBusConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.bilibili.bsql.databus.metrics.DataBusConsumerMetricConstants.CONSUMER_RECORDS_METRICS_COUNTER;
/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className DatabusFetcher.java
 * @description This is the description of DatabusFetcher.java
 * @createTime 2020-10-22 18:25:00
 */
@Slf4j
public class DataBusFetcher<T> {
    private final DataBusConsumerThread consumer;
    private final DataBusHandover handover;
    private volatile boolean running = true;
    private final SourceFunction.SourceContext<T> sourceContext;
    private final Map<Integer, DataBusTopicPartitionState<TopicPartition>> currentOffsetMap = new ConcurrentHashMap<>();
    private final DatabusSub databusSub;
    private final DeserializationSchema<T> deserializer;
    private List<Exception> exception = new LinkedList<>();
    private final Counter recordsCounter;

    public DataBusFetcher(StreamingRuntimeContext runtimeContext, SourceFunction.SourceContext<T> ctx, Properties properties,
						  DeserializationSchema<T> valueDeserializer, Map<Integer, DataBusTopicPartitionState<TopicPartition>> stateOffsetMap,
						  MetricGroup consumerMetricsGroup) throws Exception {
        this.sourceContext = ctx;
        this.handover = new DataBusHandover(runtimeContext);

		int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
		int num = getInitSum(runtimeContext, properties);
		GlobalAggregateManager aggregateManager = runtimeContext.getGlobalAggregateManager();
		if (indexOfThisSubtask < num) {
			this.databusSub = new DatabusSub(properties);
			aggregateManager.updateGlobalAggregate("sync", 1, new IntegerAggregateFunction());//客户端初始化一次，累加器加一次
			log.info("init databusSub,indexOfThisSubtask:{}, Sum:{}", indexOfThisSubtask, num);
		} else{
			this.databusSub = null;
		}

        boolean sync = true;
        while (sync) {//累加器，当所有客户端都初始化完成不再rebalance的时候再放行
            int result = aggregateManager.updateGlobalAggregate("sync", 0, new IntegerAggregateFunction());
            if (result == num) {
                sync = false;
            } else {
                log.debug("客户端总数量：{},未初始化客户端数量：{}", num, num - result);
                Thread.sleep(1000);
            }
        }
        this.consumer = new DataBusConsumerThread(runtimeContext, handover, databusSub, currentOffsetMap, stateOffsetMap, exception, consumerMetricsGroup);
        this.deserializer = valueDeserializer;
        this.recordsCounter = consumerMetricsGroup.counter(CONSUMER_RECORDS_METRICS_COUNTER);
        log.info("init DataBusFetcher finish");
    }

    public Map<Integer, DataBusTopicPartitionState<TopicPartition>> snapshotCurrentState() {
        return currentOffsetMap;
    }

    public boolean canCommit() {
    	if (databusSub == null) {
			return true;
		}
		return databusSub.backOffNoStart();
    }

    public boolean backOffFinish() {
		if (databusSub == null) {
			return false;
		}
		return databusSub.backOffFinish();
    }

    public void commitInternalOffsetsToDatabus(final ListState<Tuple2<DataBusTopicPartition, Long>> offsets) throws Exception {
		if (databusSub == null) {
			return;
		}
        for (Tuple2<DataBusTopicPartition, Long> tuple2 : offsets.get()) {
            Integer partition = tuple2.f0.getPartition();
            Long offset = tuple2.f1;
			log.info("commitInternalOffsetsToDatabus parititon:" + partition + " , offset:"+ offset);
            databusSub.commitOffset(String.valueOf(partition), String.valueOf(offset));
        }
    }

    public void runFetchLoop() throws Exception {
        try {
            consumer.start();
            while (running) {
                if (backOffFinish()) {
                    throw new RuntimeException("Redis can not connector! DataBusFetcher out!");
                }
                if(!exception.isEmpty()){
					throw new RuntimeException("poll thread exception! " , exception.get(0));
				}
                DataBusMsgInfo databusMsgInfo = handover.pollNext();
                if (databusMsgInfo != null) {
                    Object o = databusMsgInfo.getValue();
                    String s;
                    if (o instanceof JSONObject)
                        s = JSONObject.toJSONString(o);
                    else if (o instanceof JSONArray)
                        s = JSONArray.toJSONString(o);
                    else {
                        log.error("不支持的格式类型! :{}", o);
                        throw new IllegalStateException("不支持的格式类型");
                    }
                    T value = deserializer.deserialize(s.getBytes());
                    log.debug("collect:{} ", value);
                    sourceContext.collect(value);
					recordsCounter.inc();
                    DataBusTopicPartitionState<TopicPartition> kafkaTopicPartitionState = new DataBusTopicPartitionState<>(
                            new DataBusTopicPartition(databusMsgInfo.getTopic(), databusMsgInfo.getPartition()),
                            new TopicPartition(databusMsgInfo.getTopic(), databusMsgInfo.getPartition())
                    );
                    kafkaTopicPartitionState.setOffset(databusMsgInfo.getOffset());
                    currentOffsetMap.put(databusMsgInfo.getPartition(), kafkaTopicPartitionState);
                }
            }
        } finally {
            cancel();
            log.info("DataBusFetcher finish!");
        }
    }

	private int getInitSum(StreamingRuntimeContext runtimeContext, Properties properties){
		Integer partition = (Integer) properties.get(BsqlDataBusConfig.PARTITION);
		if (partition == null) {
			return runtimeContext.getNumberOfParallelSubtasks();
		}
		return Math.min(partition, runtimeContext.getNumberOfParallelSubtasks());
	}

    public void cancel() {
        running = false;
    }
}
