package org.apache.flink.bili.writer.shuffle.policy;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.bili.writer.FileSystemOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/2 4:30 下午
 */
public class DefaultSplitPolicy<T extends Row> implements SplitPolicy<T> {
	private final Logger LOG = LoggerFactory.getLogger(DefaultSplitPolicy.class);
	private final Map<String, String> properties;
	private final Map<String, Double> tableMetaMap;


	public DefaultSplitPolicy(Map<String, String> properties, Map<String, Double> tableMetaMap) {
		this.properties = properties;
		this.tableMetaMap = tableMetaMap;
	}


	@Override
	public Map<String, Tuple2<Integer, Integer>> getTableTagMap(int sinkParallelism) {
		AtomicInteger startIndex = new AtomicInteger(0);
		Map<String, Integer> directMap = getDirectMap(sinkParallelism);
		Map<String, Tuple2<Integer, Integer>> tableTagMap = directMap.entrySet()
			.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, metaTag -> {
				Tuple2<Integer, Integer> rangeTuple = Tuple2.of(startIndex.intValue(), metaTag.getValue());
				startIndex.addAndGet(metaTag.getValue());
				return rangeTuple;
			}));
		LOG.info("multi sink default split policy is :{} ", JSON.toString(tableTagMap));
		return tableTagMap;
	}

	@Override
	public String getTableTag(T row) {
		// do nothing
		return null;
	}

	protected Map<String, Integer> getDirectMap(int maxParallelism) {
		return tableMetaMap.entrySet()
			.stream()
			.collect(Collectors.toMap(Map.Entry::getKey, meta -> (int) (meta.getValue() * maxParallelism)));
	}
}
