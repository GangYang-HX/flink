package org.apache.flink.bili.writer.shuffle;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/2 3:12 下午
 */
public class SplitDataStream<T> extends DataStream<T> {
	private String tableTag;

	private Integer outputParallelism;

	private DataStream<T> splitDataStream;

	public SplitDataStream(Integer outputParallelism, DataStream<T> splitDataStream) {
		super(splitDataStream.getExecutionEnvironment(), splitDataStream.getTransformation());

	}

	public SplitDataStream(
		String tableTag,
		Integer outputParallelism,
		DataStream<T> splitDataStream) {

		super(splitDataStream.getExecutionEnvironment(), splitDataStream.getTransformation());
		this.tableTag = tableTag;
		this.outputParallelism = outputParallelism;
		this.splitDataStream = splitDataStream;
	}

	public DataStream<T> getSplitDataStream() {
		return splitDataStream;
	}

	public Integer getOutputParallelism() {
		return outputParallelism;
	}

	public String getTableTag() {
		return tableTag;
	}
}
