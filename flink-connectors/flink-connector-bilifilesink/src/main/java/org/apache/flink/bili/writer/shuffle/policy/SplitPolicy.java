package org.apache.flink.bili.writer.shuffle.policy;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Map;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/2 3:20 下午
 */
public interface SplitPolicy<T extends Row> extends Serializable {

	Map<String, Tuple2<Integer, Integer>> getTableTagMap(int sinkParallelism);

	String getTableTag(T row) throws Exception;



}
