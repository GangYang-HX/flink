package org.apache.flink.bili.writer.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.bili.writer.*;
import org.apache.flink.bili.writer.metastorefactory.MultiHiveTableMetaStoreFactory;
import org.apache.flink.bili.writer.metastorefactory.TableMetaStoreFactory;
import org.apache.flink.bili.writer.partition.PartitionComputer;
import org.apache.flink.bili.writer.shuffle.MultiSplitOperator;
import org.apache.flink.bili.writer.shuffle.SplitDataStream;
import org.apache.flink.bili.writer.shuffle.policy.SplitPolicy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.bili.writer.FileSystemOptions.*;
import static org.apache.flink.table.connector.ConnectorValues.DEFAULT_PARALLEL;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/28 9:30 下午
 */
public class MultiStreamFileSystemSink<T, S extends MultiStreamFileSystemSink<T, S>> {
	private static final Logger LOG = LoggerFactory.getLogger(MultiStreamFileSystemSink.class);

	protected final Map<String, ObjectIdentifier> tableIdentifierMap;

	protected final List<String> partitionKeys;
	protected final Path path;
	protected final Map<String, String> properties;

	/**
	 * Saber related attribute.
	 */
	protected Object writer;
	protected PartitionComputer<T> partitionComputer;
	protected BucketAssigner<T, String> assigner;
	protected OutputFileConfig outputFileConfig;
	protected Map<String, Double> tableMetaMap;
	protected SplitPolicy splitPolicy;

	/**
	 * Construct a file system table sink.
	 *
	 * @param path       directory path of the file system table.
	 * @param properties properties.
	 */
	public MultiStreamFileSystemSink(
		Path path,
		Map<String, String> properties,
		List<String> partitionKeys,
		Map<String, ObjectIdentifier> tableIdentifierMap
	) {

		this.path = path;
		this.properties = properties;

		this.partitionKeys = partitionKeys;
		this.tableIdentifierMap = tableIdentifierMap;
		this.outputFileConfig = OutputFileConfig.builder().build();
	}

	protected S self() {
		return (S) this;
	}

	public S withWriter(Encoder<T> encoderWriter) {
		this.writer = encoderWriter;
		return self();
	}

	public S withWriter(BulkWriter.Factory<T> bulkWriter) {
		this.writer = bulkWriter;
		return self();
	}

	@Deprecated
	public S withPartitionComputer(PartitionComputer<T> partitionComputer) {
		this.partitionComputer = partitionComputer;
		return self();
	}

	public S withBucketAssigner(BucketAssigner<T, String> assigner) {
		this.assigner = assigner;
		return self();
	}

	public S withOutputFileConfig(OutputFileConfig outputFileConfig) {
		this.outputFileConfig = outputFileConfig;
		return self();
	}


	public S withTableMeta(Map<String, Double> tableMetaMap) {
		this.tableMetaMap = tableMetaMap;
		return self();

	}

	public S withSplitPolicy(SplitPolicy<Row> splitPolicy) {
		this.splitPolicy = splitPolicy;
		return self();
	}

	public DataStreamSink<?> multiSinkDataStream(DataStream<T> dataStream) {
		Configuration conf = new Configuration();
		properties.forEach(conf::setString);
		TableMetaStoreFactory metaStoreFactory =
			new MultiHiveTableMetaStoreFactory(null, conf.getString(SINK_HIVE_VERSION));
//		TableBucketAssigner assigner = new TableBucketAssigner(this.partitionComputer);
		StreamFileSystemSink.TableRollingPolicy rollingPolicy = new StreamFileSystemSink.TableRollingPolicy(
			!(writer instanceof Encoder),
			conf.getLong(SINK_ROLLING_POLICY_FILE_SIZE),
			conf.getLong(SINK_ROLLING_POLICY_TIME_INTERVAL));
		StreamingFileSink.BucketsBuilder<T, ?, ? extends StreamingFileSink.BucketsBuilder<T, ?, ?>> bucketsBuilder;
		InactiveBucketListener listener = new InactiveBucketListener();
		if (writer instanceof Encoder) {
			//noinspection unchecked
			bucketsBuilder = StreamingFileSink.forRowFormat(
				path, (Encoder<T>) writer)
				.withBucketAssigner(assigner)
				.withRollingPolicy(rollingPolicy)
				.withOutputFileConfig(outputFileConfig);
		} else {
			bucketsBuilder = StreamingFileSink.forBulkFormat(
				path, (BulkWriter.Factory<T>) writer)
				.withBucketAssigner(assigner)
				//.withBucketLifeCycleListener(listener)
				.withRollingPolicy(rollingPolicy)
				.withOutputFileConfig(outputFileConfig);
		}
		return createAdaptiveStreamingSink(
			conf,
			path,
			partitionKeys,
			tableIdentifierMap,
			dataStream,
			bucketsBuilder,
			listener,
			metaStoreFactory,
			writer,
			splitPolicy);
	}


	public static DataStreamSink createAdaptiveStreamingSink(
		Configuration conf,
		Path path,
		List<String> partitionKeys,
		Map<String, ObjectIdentifier> tableIdentifierMap,
		DataStream inputStream,
		StreamingFileSink.BucketsBuilder bucketsBuilder,
		InactiveBucketListener listener,
		TableMetaStoreFactory msFactory,
		Object metricSetter,
		SplitPolicy splitPolicy) {
		MultiSplitOperator<? extends Row> multiSplitOperator = MultiSplitOperator.create(conf, splitPolicy,
			inputStream.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
				inputStream.getParallelism() : inputStream.getExecutionConfig().getParallelism());
		Collection<? extends SplitDataStream<? extends Row>> split =
			multiSplitOperator.split(inputStream);

		List<DataStreamSink> dataStreamCollects =
			split.stream()
				.map(
					splittedDataStream ->
						createStreamingSink(
							conf,
							path,
							partitionKeys,
							tableIdentifierMap,
							splittedDataStream.getSplitDataStream(),
							bucketsBuilder,
							listener,
							msFactory,
							metricSetter))
				.collect(Collectors.toList());

		return dataStreamCollects.get(0);
	}

	public static DataStreamSink createStreamingSink(
		Configuration conf,
		Path path,
		List<String> partitionKeys,
		Map<String, ObjectIdentifier> tableIdentifierMap,
		DataStream inputStream,
		StreamingFileSink.BucketsBuilder bucketsBuilder,
		InactiveBucketListener listener,
		TableMetaStoreFactory msFactory,
		Object metricSetter) {

		StreamingFileWriter fileWriter = new StreamingFileWriter(
			StreamingFileSink.BucketsBuilder.DEFAULT_BUCKET_CHECK_INTERVAL, bucketsBuilder, listener, metricSetter, conf);
		DataStream<StreamingFileCommitter.CommitMessage> writerStream;

		if (DEFAULT_PARALLEL == conf.getInteger(SINK_WRITER_PARALLEL)) {
			writerStream = inputStream
				.transform(
					StreamingFileWriter.class.getSimpleName(),
					StreamingFileWriter.class.getSimpleName(),
					TypeExtractor.createTypeInfo(StreamingFileCommitter.CommitMessage.class),
					fileWriter)
				.setParallelism(inputStream.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
					inputStream.getParallelism() : inputStream.getExecutionConfig().getParallelism());
		} else {
			writerStream = inputStream
				.transform(
					StreamingFileWriter.class.getSimpleName(),
					StreamingFileWriter.class.getSimpleName(),
					TypeExtractor.createTypeInfo(StreamingFileCommitter.CommitMessage.class),
					fileWriter)
				.setParallelism(conf.getInteger(SINK_WRITER_PARALLEL));
		}

		DataStream<?> returnStream = writerStream;

		// save committer when we don't need it.
//		if (partitionKeys.size() > 0 && conf.contains(SINK_PARTITION_COMMIT_POLICY_KIND)) {
		if (conf.contains(SINK_PARTITION_COMMIT_POLICY_KIND)) {
			MultiStreamingFileCommitter committer = new MultiStreamingFileCommitter(
				path, tableIdentifierMap, partitionKeys, msFactory, conf);
			returnStream = writerStream
				.transform(StreamingFileCommitter.class.getSimpleName(), StreamingFileCommitter.class.getSimpleName(), Types.VOID, committer)
				.setParallelism(1)
				.setMaxParallelism(1);
		}
		//noinspection unchecked
		return returnStream.addSink(new DiscardingSink()).setParallelism(1);
	}


}
