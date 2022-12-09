package com.bilibili.bsql.databus.fetcher;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lombok.Data;

import static com.bilibili.bsql.databus.metrics.DataBusConsumerMetricConstants.CONSUMER_SKIP_RECORDS_METRICS_COUNTER;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className DataBusConsumerThread.java
 * @description This is the description of DataBusConsumerThread.java
 * @createTime 2020-10-22 18:26:00
 */
public class DataBusConsumerThread extends Thread {
	private final Logger logger = LoggerFactory.getLogger(DataBusConsumerThread.class);
	private final DataBusHandover handover;
	private final DatabusSub databusSub;
	private volatile boolean running = true;
	private long lastCommitOffsetTime = 0;
	/**
	 * 当前offset，用于无checkpoint时offset提交用
	 */
	private final Map<Integer, DataBusTopicPartitionState<TopicPartition>> currentOffsetMap;
	/**
	 * 从上一次checkpoint恢复出来的offset，用于判断是否需要跳过数据
	 */
	private final Map<Integer, DataBusTopicPartitionState<TopicPartition>> stateOffsetMap;
	private final PartitionOffsetCheck partitionOffsetCheck;
	private final boolean isCheckpointingEnabled;
	private List<Exception> exception;
	private final boolean exactlyOnce;
	private Counter skipRecordsCounter;

	public DataBusConsumerThread(StreamingRuntimeContext runtimeContext, DataBusHandover handover, DatabusSub databusSub,
								 Map<Integer, DataBusTopicPartitionState<TopicPartition>> currentOffsetMap,
								 Map<Integer, DataBusTopicPartitionState<TopicPartition>> stateOffsetMap, List<Exception> exception,
								 MetricGroup consumerMetricsGroup) {
		super("databus consumer for" + runtimeContext.getTaskNameWithSubtasks());
		this.handover = handover;
		this.databusSub = databusSub;
		this.isCheckpointingEnabled = runtimeContext.isCheckpointingEnabled();
		this.currentOffsetMap = currentOffsetMap;
		this.partitionOffsetCheck = new PartitionOffsetCheck(stateOffsetMap);
		this.stateOffsetMap = stateOffsetMap;
		this.exception = exception;
		this.exactlyOnce = CheckpointingMode.EXACTLY_ONCE.equals(runtimeContext.getCheckpointMode());
		this.skipRecordsCounter = consumerMetricsGroup.counter(CONSUMER_SKIP_RECORDS_METRICS_COUNTER);
	}

	@Override
	public void run() {
		try {
			while (running && databusSub != null && !databusSub.backOffFinish()) {
				List<DataBusMsgInfo> databusMsgInfoList = exactlyOnce ? databusSub.poll() : databusSub.pollWithRetry();

				if (isCheckpointingEnabled) {
					partitionOffsetCheck.skipOffset(databusMsgInfoList);
				}
				handover.produce(databusMsgInfoList);
				long now = System.currentTimeMillis();
				long expend = now - lastCommitOffsetTime;
				if (expend > 5000 && !isCheckpointingEnabled) {//每隔至少5s提交一次offset
					for (Map.Entry<Integer, DataBusTopicPartitionState<TopicPartition>> entry : currentOffsetMap.entrySet()) {
						DataBusTopicPartitionState<TopicPartition> stat = entry.getValue();
						databusSub.commitOffset(String.valueOf(stat.getPartition()), String.valueOf(stat.getOffset()));
					}
					lastCommitOffsetTime = now;
				}
			}
		} catch (Exception e) {
			logger.error("consumer databus error: ", e);
			exception.add(e);
		} finally {
			shutdown();
			logger.info("DatabusConsumerThread closed!");
		}
	}

	public void shutdown() {
		running = false;
	}


	@Data
	class PartitionOffsetCheck {

		/**
		 * 是否需要检查offset，key为partition，value为true则需要检查，value为false则不需要检查
		 */
		private Map<Integer, Boolean> offsetCheckMap;
		private int skipCount = 0;

		public PartitionOffsetCheck(Map<Integer, DataBusTopicPartitionState<TopicPartition>> stateOffsetMap) {
			this.offsetCheckMap = new HashMap<>();
			if (stateOffsetMap == null || stateOffsetMap.isEmpty()) {
				logger.info("do not start skip partition offset");
				return;
			}
			stateOffsetMap.forEach((k, v) -> {
				offsetCheckMap.put(k, true);
				logger.info("PartitionOffsetCheck partition:"+ k + "value:" + v);
			});
		}

		public List<DataBusMsgInfo> skipOffset(List<DataBusMsgInfo> databusMsgInfoList) {
			if (databusMsgInfoList == null) {
				return null;
			}
			Iterator<DataBusMsgInfo> iterator = databusMsgInfoList.iterator();
			while (iterator.hasNext()) {
				DataBusMsgInfo dataBusMsgInfo = iterator.next();
				if (!offsetCheckMap.containsKey(dataBusMsgInfo.getPartition()) || !offsetCheckMap.get(dataBusMsgInfo.getPartition())) {
					//map中不存在此partition 或者 map中已经标记为跳过 则跳过。
					continue;
				}

				long stateOffset = stateOffsetMap.get(dataBusMsgInfo.getPartition()).getOffset();
				if (dataBusMsgInfo.getOffset() <= stateOffset) {
					logger.info("skip partition offset:" + dataBusMsgInfo.getPartition() + " " + dataBusMsgInfo.getOffset());
					skipCount++;
					skipRecordsCounter.inc();
					iterator.remove();
				} else {
					logger.info("finish skip partition offset:" + dataBusMsgInfo.getPartition() + " " + dataBusMsgInfo.getOffset() + "stateOffset:" + stateOffset );
					offsetCheckMap.put(dataBusMsgInfo.getPartition(), false);
				}
			}
			return databusMsgInfoList;
		}

	}
}
