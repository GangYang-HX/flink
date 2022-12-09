package org.apache.flink.bili.writer.shuffle;

import net.minidev.json.JSONObject;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.bili.writer.shuffle.policy.SplitPolicy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/2 3:17 下午
 */
public class AdaptiveMultiSplitOperator<T extends Row>
	implements KeySelector<T, Integer>, MultiSplitOperator<T> {
	private static final Logger LOG = LoggerFactory.getLogger(AdaptiveMultiSplitOperator.class);
	public SplitPolicy<T> splitPolicy;
	Map<String, Tuple2<Integer, Integer>> defaultTableTagMap;
	private final int sinkParallelism;

	public AdaptiveMultiSplitOperator(SplitPolicy<T> splitPolicy, int sinkParallelism) {
		this.splitPolicy = splitPolicy;
		this.defaultTableTagMap = splitPolicy.getTableTagMap(sinkParallelism);
		this.sinkParallelism = sinkParallelism;

	}

	@Override
	public String getTableTag(T value) throws Exception {
		return splitPolicy.getTableTag(value);
	}

	@Override
	public Integer getKey(T value) throws Exception {
		String tableTag = getTableTag(value);
		Tuple2<Integer, Integer> tuple2 = this.defaultTableTagMap.get(tableTag);
		if (tuple2 == null) {
			LOG.warn("table tag :{} is not in default table tag map:{}", tableTag, defaultTableTagMap);
			return (int) (Math.random() * sinkParallelism);
		}
		int index = tuple2.f0 + (int) (Math.random() * tuple2.f1);
		if (index >= sinkParallelism) {
			LOG.warn("custom multi sink index is out of range : {} , parallelism is : {}.", index, sinkParallelism);
			index = (int) (Math.random() * sinkParallelism);
		}
		return index;

	}

	@Override
	public Collection<SplitDataStream<T>> split(DataStream<T> inputStream) {
		DataStream<T> rowDataStream =
			inputStream.partitionCustom((key, partitionIndex) -> key, this);
		rowDataStream.getTransformation().setName("MultiSplit");

		int maxSinkParallelism = getMaxSinkParallelism();
		int sinkParallelism = Math.min(rowDataStream.getParallelism(), maxSinkParallelism);

		return Collections.singletonList(
			new SplitDataStream<>(null, sinkParallelism, rowDataStream));
	}

	private int getMaxSinkParallelism() {
		return defaultTableTagMap.values().stream().mapToInt(integerIntegerTuple2 -> integerIntegerTuple2.f1).sum();
	}

}
