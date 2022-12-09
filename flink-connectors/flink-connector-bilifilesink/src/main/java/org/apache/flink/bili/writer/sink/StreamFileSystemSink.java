/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.bili.writer.sink;

import java.io.IOException;
import java.util.*;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.bili.writer.ObjectIdentifier;
import org.apache.flink.bili.writer.StreamingFileCommitter;
import org.apache.flink.bili.writer.StreamingFileWriter;
import org.apache.flink.bili.writer.*;
import org.apache.flink.bili.writer.metastorefactory.BsqlHiveTableMetaStoreFactory;
import org.apache.flink.bili.writer.metastorefactory.TableMetaStoreFactory;
import org.apache.flink.bili.writer.partition.PartitionComputer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BucketsBuilder;

import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.flink.bili.writer.FileSystemOptions.*;
import static org.apache.flink.table.connector.ConnectorValues.DEFAULT_PARALLEL;

/**
 * File system {@link TableSink}.
 */
public class StreamFileSystemSink<T, S extends StreamFileSystemSink<T, S>>  {

    private static final Logger LOG = LoggerFactory.getLogger(StreamFileSystemSink.class);

    protected final ObjectIdentifier tableIdentifier;
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
	protected Map<String, String> tableMetaMap;

	/**
	 * Construct a file system table sink.
	 *
	 * @param path       directory path of the file system table.
	 * @param properties properties.
	 */
	public StreamFileSystemSink(
		Path path,
		Map<String, String> properties,
		ArrayList<String> partitionKeys,
		ObjectIdentifier tableIdentifier
	) {

		this.path = path;
		this.properties = properties;

		this.partitionKeys = partitionKeys;
		this.tableIdentifier = tableIdentifier;
		this.outputFileConfig = OutputFileConfig.builder().build();
	}

	protected S self() {
		return (S) this;
	}

	public S withWriter(Object writer) {
		this.writer = writer;
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


	public S withTableMeta(Map<String, String> tableMetaMap) {
		this.tableMetaMap = tableMetaMap;
		return self();

	}


	public DataStreamSink<?> sinkDataStream(DataStream<T> dataStream) {
		Configuration conf = new Configuration();
		properties.forEach(conf::setString);
		TableMetaStoreFactory metaStoreFactory = new BsqlHiveTableMetaStoreFactory(null,
				conf.getString(SINK_HIVE_VERSION),
				tableIdentifier.getDatabaseName(),
				tableIdentifier.getObjectName(),
				conf.getString(SINK_HIVE_TABLE_FORMAT),
				conf.getString(SINK_HIVE_TABLE_COMPRESS));
//		TableBucketAssigner assigner = new TableBucketAssigner(this.partitionComputer);
		TableRollingPolicy rollingPolicy = new TableRollingPolicy(
			!(writer instanceof Encoder),
			conf.getLong(SINK_ROLLING_POLICY_FILE_SIZE),
			conf.getLong(SINK_ROLLING_POLICY_TIME_INTERVAL));
		LOG.info("IS_SINK_ROLLING_POLICY_ROLLING_ON_CHECKPOINT {}, SINK_ROLLING_POLICY_FILE_SIZE is {},SINK_ROLLING_POLICY_TIME_INTERVAL is {}",!(writer instanceof Encoder), conf.getLong(SINK_ROLLING_POLICY_FILE_SIZE), conf.getLong(SINK_ROLLING_POLICY_TIME_INTERVAL));
		BucketsBuilder<T, ?, ? extends BucketsBuilder<T, ?, ?>> bucketsBuilder;
		InactiveBucketListener listener = new InactiveBucketListener();
		if (writer instanceof Encoder) {
			//noinspection unchecked
			bucketsBuilder = StreamingFileSink.forRowFormat(
				path, (Encoder<T>) writer)
				.withBucketAssigner(assigner)
				//.withBucketLifeCycleListener(listener)
				.withRollingPolicy(rollingPolicy)
				.withOutputFileConfig(outputFileConfig);
		} else {
			//noinspection unchecked
			//todo: bulkWriter 就用原来那种checkpoint型的
			bucketsBuilder = StreamingFileSink.forBulkFormat(
				path, (BulkWriter.Factory<T>) writer)
				.withBucketAssigner(assigner)
				//.withBucketLifeCycleListener(listener)
				.withRollingPolicy(rollingPolicy)
				.withOutputFileConfig(outputFileConfig);
		}
		return createStreamingSink(
			conf,
			path,
			partitionKeys,
			tableIdentifier,
			dataStream,
			bucketsBuilder,
			listener,
			metaStoreFactory,
			writer);
	}

    public static DataStreamSink createStreamingSink(
            Configuration conf,
            Path path,
            List<String> partitionKeys,
            ObjectIdentifier tableIdentifier,
            DataStream inputStream,
            BucketsBuilder bucketsBuilder,
            InactiveBucketListener listener,
            TableMetaStoreFactory msFactory,
            Object metricSetter) {

        StreamingFileWriter fileWriter = new StreamingFileWriter(
                BucketsBuilder.DEFAULT_BUCKET_CHECK_INTERVAL, bucketsBuilder, listener, metricSetter, conf);
        DataStream<StreamingFileCommitter.CommitMessage> writerStream;

        if (DEFAULT_PARALLEL == conf.getInteger(SINK_WRITER_PARALLEL)) {
            writerStream = inputStream
                .transform(
                    StreamingFileWriter.class.getSimpleName(),
					StreamingFileWriter.class.getSimpleName(),
					TypeExtractor.createTypeInfo(StreamingFileCommitter.CommitMessage.class),
                    fileWriter)
                .setParallelism(inputStream.getTransformation().getParallelism())
                .slotSharingGroup(inputStream.getTransformation().getSlotSharingGroup());
        } else {
            writerStream = inputStream
                .transform(
                    StreamingFileWriter.class.getSimpleName(),
					StreamingFileWriter.class.getSimpleName(),
					TypeExtractor.createTypeInfo(StreamingFileCommitter.CommitMessage.class),
                    fileWriter)
                .setParallelism(conf.getInteger(SINK_WRITER_PARALLEL))
                .slotSharingGroup(inputStream.getTransformation().getSlotSharingGroup());
        }

        DataStream<?> returnStream = writerStream;

        // save committer when we don't need it.
//		if (partitionKeys.size() > 0 && conf.contains(SINK_PARTITION_COMMIT_POLICY_KIND)) {
        if (conf.contains(SINK_PARTITION_COMMIT_POLICY_KIND)) {
            StreamingFileCommitter committer = new StreamingFileCommitter(
                    path, tableIdentifier, partitionKeys, msFactory, conf);
            returnStream = writerStream
                    .transform(StreamingFileCommitter.class.getSimpleName(), StreamingFileCommitter.class.getSimpleName(), Types.VOID, committer)
                    .setParallelism(1)
                    .setMaxParallelism(1)
                    .slotSharingGroup(writerStream.getTransformation().getSlotSharingGroup());
        }
        //noinspection unchecked
        return returnStream.addSink(new DiscardingSink()).setParallelism(1)
                .slotSharingGroup(returnStream.getTransformation().getSlotSharingGroup());
    }

	private Path toStagingPath() {
		Path stagingDir = new Path(path, ".staging_" + System.currentTimeMillis());
		try {
			FileSystem fs = stagingDir.getFileSystem();
			Preconditions.checkState(
					fs.exists(stagingDir) || fs.mkdirs(stagingDir),
					"Failed to create staging dir " + stagingDir);
			return stagingDir;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}



	/**
	 * Table {@link RollingPolicy}, it extends {@link CheckpointRollingPolicy} for bulk writers.
	 */
	public static class TableRollingPolicy<T, S extends String> extends CheckpointRollingPolicy<T, String> {

		private final boolean rollOnCheckpoint;
		private final long rollingFileSize;
		private final long rollingTimeInterval;

		public TableRollingPolicy(
				boolean rollOnCheckpoint,
				long rollingFileSize,
				long rollingTimeInterval) {
			this.rollOnCheckpoint = rollOnCheckpoint;
			Preconditions.checkArgument(rollingFileSize > 0L);
			Preconditions.checkArgument(rollingTimeInterval > 0L);
			this.rollingFileSize = rollingFileSize;
			this.rollingTimeInterval = rollingTimeInterval;
		}

		@Override
		public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
			try {
				if (rollOnCheckpoint || partFileState.getSize() > rollingFileSize) {
					LOG.info("BucketId is {}, fileSize is {}, shouldRollOnCheckpoint happened", partFileState.getBucketId(), partFileState.getSize());
					return true;
				}
				return false;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean shouldRollOnEvent(
				PartFileInfo<String> partFileState,
				T element) throws IOException {
			if (partFileState.getSize() > rollingFileSize) {
				LOG.info("BucketId is {}, file size over {}, shouldRollOnEvent happened", partFileState.getBucketId(), rollingFileSize);
				return true;
			}
			return false;
		}

		@Override
		public boolean shouldRollOnProcessingTime(
				PartFileInfo<String> partFileState,
				long currentTime) {
			if (currentTime - partFileState.getCreationTime() >= rollingTimeInterval){
				LOG.info("BucketId is {}, file createTime is {}, shouldRollOnProcessingTime happened", partFileState.getBucketId(), partFileState.getCreationTime());
				return true;
			}
			return false;
		}
	}

}
