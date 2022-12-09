package org.apache.flink.bili.writer.shuffle;

import org.apache.flink.bili.writer.FileSystemOptions;
import org.apache.flink.bili.writer.shuffle.policy.SplitPolicy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/2 3:09 下午
 */
public interface MultiSplitOperator<T extends Row> {

	String ADAPTIVE_PARTITIONER = "adaptive-partitioner";

	String getTableTag(T value) throws Exception;

	Collection<SplitDataStream<T>> split(DataStream<T> inputStream) ;

	static <T extends Row> MultiSplitOperator<T> create(Configuration conf, SplitPolicy splitPolicy, Integer parallelism) {

		String optimizeType =
			Preconditions.checkNotNull(
				conf.getString(FileSystemOptions.SINK_PARTITION_OPTIMIZE_TYPE),
				"sink partition optimize type");
		switch (optimizeType) {
			case ADAPTIVE_PARTITIONER:
				return new AdaptiveMultiSplitOperator<>(splitPolicy, parallelism);

			default:
				throw new UnsupportedOperationException(
					"Unsupported sink partition optimize type: " + optimizeType);
		}
	}

}
