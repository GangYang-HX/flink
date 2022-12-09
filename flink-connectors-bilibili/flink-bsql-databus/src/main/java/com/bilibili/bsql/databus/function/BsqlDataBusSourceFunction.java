package com.bilibili.bsql.databus.function;

import com.bilibili.bsql.databus.fetcher.DataBusFetcher;
import com.bilibili.bsql.databus.fetcher.DataBusTopicPartition;
import com.bilibili.bsql.databus.fetcher.DataBusTopicPartitionState;
import com.bilibili.bsql.databus.tableinfo.BsqlDataBusConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.kafka.common.TopicPartition;
import pleiades.component.model.DatabusClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.bilibili.bsql.databus.metrics.DataBusConsumerMetricConstants.DATABUS_CONSUMER_METRICS_GROUP;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className DataBusSourceFunction.java
 * @description This is the description of DataBusSourceFunction.java
 * @createTime 2020-10-22 19:29:00
 */
@Slf4j
public class BsqlDataBusSourceFunction<T> extends RichParallelSourceFunction<T> implements CheckpointedFunction, CheckpointListener {
    private static final long serialVersionUID = 715410501709412211L;
    private volatile boolean isRunning = true;
    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";
    private volatile DataBusFetcher<T> fetcher;
    private transient ListState<Tuple2<DataBusTopicPartition, Long>> unionOffsetStates;
    private final Properties properties;
    private final DeserializationSchema<T> valueDeserializer;

    public BsqlDataBusSourceFunction(Properties properties, DeserializationSchema<T> valueDeserializer) {
        this.properties = properties;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
		unionOffsetStates.clear();
        if (fetcher != null) {
			Map<Integer, DataBusTopicPartitionState<TopicPartition>> snapshotOffsetMap = new HashMap<>(fetcher.snapshotCurrentState());
			for (Map.Entry<Integer, DataBusTopicPartitionState<TopicPartition>> entry : snapshotOffsetMap.entrySet()) {
				log.info("databus snapshotState parititon:" + entry.getValue().getDatabusTopicPartition() + " , offset:"+ entry.getValue().getOffset());
				unionOffsetStates.add(Tuple2.of(entry.getValue().getDatabusTopicPartition(), entry.getValue().getOffset()));
			}
        }
    }

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (fetcher != null) {
			if (fetcher.canCommit()) {
				fetcher.commitInternalOffsetsToDatabus(unionOffsetStates);
			} else {
				log.warn("Redis connect error, can not commit offset.");
			}
		}
	}

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        try {
            DatabusClient.register();
        } catch (Exception e) {
            log.warn("DatabusClient has register");
        }
        OperatorStateStore stateStore = context.getOperatorStateStore();
        ListState<Tuple2<DataBusTopicPartition, Long>> oldRoundRobinListState =
                stateStore.getListState(new ListStateDescriptor(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME, new JavaSerializer()));
        this.unionOffsetStates = stateStore.getUnionListState(
                new ListStateDescriptor<>(OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<DataBusTopicPartition, Long>>() {
                }))
        );
        if (context.isRestored()) {
            for (Tuple2<DataBusTopicPartition, Long> kafkaOffset : oldRoundRobinListState.get()) {
                unionOffsetStates.add(kafkaOffset);
            }
            oldRoundRobinListState.clear();
        }

		printuUnionOffsetStates(unionOffsetStates);
    }

    private void printuUnionOffsetStates(ListState<Tuple2<DataBusTopicPartition, Long>> unionOffsetStates) throws Exception {
		unionOffsetStates.get().forEach( entry -> {
			Integer partition = entry.f0.getPartition();
			Long offset = entry.f1;
			log.info("initializeState parititon:" + partition + " , offset:"+ offset);
		});
	}

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        if (!isRunning) {
            return;
        }
		Map<Integer, DataBusTopicPartitionState<TopicPartition>> stateOffsetMap = convert(unionOffsetStates);
		fetcher = createFetcher((StreamingRuntimeContext) getRuntimeContext(), ctx, properties, valueDeserializer, stateOffsetMap);
		fetcher.runFetchLoop();
    }

    private DataBusFetcher<T> createFetcher(StreamingRuntimeContext runtimeContext,
                                            SourceContext<T> ctx, Properties properties,
                                            DeserializationSchema<T> valueDeserializer,
											Map<Integer, DataBusTopicPartitionState<TopicPartition>> stateOffsetMap) throws Exception {
        return new DataBusFetcher<>(runtimeContext, ctx, properties, valueDeserializer, stateOffsetMap, runtimeContext.getMetricGroup().addGroup(DATABUS_CONSUMER_METRICS_GROUP));
    }

    @Override
    public void cancel() {
        if (fetcher != null) {
            fetcher.cancel();
        }
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        cancel();
    }

	private Map<Integer, DataBusTopicPartitionState<TopicPartition>> convert(ListState<Tuple2<DataBusTopicPartition, Long>> unionOffsetStates) throws Exception {
		Map<Integer, DataBusTopicPartitionState<TopicPartition>> stateOffsetMap =  new HashMap<>();
		for (Tuple2<DataBusTopicPartition, Long> tuple2 : unionOffsetStates.get()) {
			Integer partition = tuple2.f0.getPartition();
			Long offset = tuple2.f1;
			String topic = tuple2.f0.getTopic();
			DataBusTopicPartitionState<TopicPartition> kafkaTopicPartitionState = new DataBusTopicPartitionState<>(
				new DataBusTopicPartition(topic, partition),
				new TopicPartition(topic, partition)
			);
			kafkaTopicPartitionState.setOffset(offset);
			stateOffsetMap.put(partition, kafkaTopicPartitionState);
		}
		return stateOffsetMap;
	}

}
