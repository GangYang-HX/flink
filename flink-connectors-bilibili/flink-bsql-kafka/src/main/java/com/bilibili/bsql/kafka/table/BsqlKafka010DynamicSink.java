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

package com.bilibili.bsql.kafka.table;

import java.util.*;
import java.util.stream.Stream;

import com.bilibili.bsql.kafka.producer.DiscoverKafkaProducer;
import com.bilibili.bsql.kafka.ability.metadata.write.WritableMetadata;
import com.bilibili.bsql.kafka.sink.HashKeyBytesSerializationSchemaWrapper;
import com.bilibili.bsql.kafka.sink.HashKeyProtoBytesSerializationSchemaWrapper;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSinkBase;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProviderWithParallel;
import org.apache.flink.table.connector.sink.abilities.SupportsWriteMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;


import com.bilibili.bsql.common.format.CustomDelimiterSerialization;
import com.bilibili.bsql.kafka.producer.BsqlKafkaProducer;
import com.bilibili.bsql.kafka.producer.RetryKafkaProducer;
import com.bilibili.bsql.kafka.sink.HashKeySerializationSchemaWrapper;

/**
 * Kafka 0.10 table sink for writing data into Kafka.
 *
 * in saber:
 * KafkaSink -> ParallelKafkaTableSink -> SaberKafkaProducer
 *
 *
 */
@Internal
public class BsqlKafka010DynamicSink extends KafkaDynamicSinkBase implements SupportsWriteMetadata {

	protected final Integer parallel;
	protected final Integer primaryKeyIndex;
	protected final Boolean kafkaFailRetry;
	protected final String serializer;
	private static final String BYTES = "bytes";
	private static final String PROTO_BYTES = "proto_bytes";
	protected List<String> metadataKeys;
	protected int[] metadataProjectIndices;
	protected final DataType physicalDataType;

	public BsqlKafka010DynamicSink(
			DataType consumedDataType,
			DataType physicalDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			Integer parallel,
			Integer primaryKeyIndex,
			Boolean kafkaFailRetry,
			String serializer) {
		super(
			consumedDataType,
			topic,
			properties,
			partitioner,
			encodingFormat);
		this.parallel = parallel;
		this.primaryKeyIndex = primaryKeyIndex;
		this.kafkaFailRetry = kafkaFailRetry;
		this.serializer = serializer;
		this.physicalDataType = physicalDataType;
		this.metadataKeys = Collections.emptyList();
		this.metadataProjectIndices = new int[]{};
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (RowKind kind : requestedMode.getContainedKinds()) {
			if (kind != RowKind.UPDATE_BEFORE) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	@Override
	protected FlinkKafkaProducerBase<RowData> createKafkaProducer(
			String topic,
			Properties properties,
			SerializationSchema<RowData> serializationSchema,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner) {
		return new DiscoverKafkaProducer<>(
			topic,
			serializationSchema,
			properties,
			partitioner.orElse(null));
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		HashKeySerializationSchemaWrapper schemaWrapper = generateSchemaWrapper(context);
		SinkFunction<RowData> kafkaProducer = createKafkaProducer(schemaWrapper);
		((FlinkKafkaProducer010<RowData>) kafkaProducer).setLogFailuresOnly(true);
		return SinkFunctionProviderWithParallel.of(kafkaProducer, parallel);
	}

	public SinkFunction<RowData> createKafkaProducer(HashKeySerializationSchemaWrapper schemaWrapper) {
		final SinkFunction<RowData> kafkaProducer;
		if (this.kafkaFailRetry) {
			kafkaProducer = new RetryKafkaProducer(
				this.topic,
				schemaWrapper,
				this.properties,
				this.partitioner.orElse(null),
				1);
		} else{
			kafkaProducer = new BsqlKafkaProducer(
				this.topic,
				schemaWrapper,
				this.properties,
				this.partitioner.orElse(null));
		}
		return kafkaProducer;
	}

	public HashKeySerializationSchemaWrapper generateSchemaWrapper(Context context) {
		DataStructureConverter converter = context.createDataStructureConverter(consumedDataType);
		SerializationSchema<RowData> serializationSchema = this.encodingFormat.createRuntimeEncoder(context, this.physicalDataType);
//		CustomDelimiterSerialization serializationSchema =
//			(CustomDelimiterSerialization) this.encodingFormat.createRuntimeEncoder(context, this.physicalDataType);
		if (BYTES.equals(this.serializer)) {
			return new HashKeyBytesSerializationSchemaWrapper(
				(CustomDelimiterSerialization) serializationSchema,
				primaryKeyIndex,
				converter,
				metadataProjectIndices,
				metadataKeys);
        } else if (PROTO_BYTES.equals(this.serializer)) {
			if (serializationSchema instanceof PbRowDataSerializationSchema) {
				return new HashKeyProtoBytesSerializationSchemaWrapper(
					(PbRowDataSerializationSchema) serializationSchema,
					primaryKeyIndex,
					converter,
					metadataProjectIndices,
					metadataKeys);
			}
        }
		return new HashKeySerializationSchemaWrapper((
			CustomDelimiterSerialization)
			serializationSchema,
			primaryKeyIndex,
			converter,
			metadataProjectIndices,
			metadataKeys);
	}

	@Override
	public DynamicTableSink copy() {
		return new BsqlKafka010DynamicSink(
			this.consumedDataType,
			this.physicalDataType,
			this.topic,
			this.properties,
			this.partitioner,
			this.encodingFormat,
			this.parallel,
			this.primaryKeyIndex,
			this.kafkaFailRetry,
			this.serializer
		);
	}

	@Override
	public String asSummaryString() {
		return "Kafka 0.10 table sink";
	}

	@Override
	public void applyWriteMetadata(List<String> metadataKeys, int[] metadataProjectIndices) {
		this.metadataKeys = metadataKeys;
		metadataKeys.forEach(k -> Stream.of(WritableMetadata.values())
			.filter(wm -> wm.key.equals(k))
			.findFirst()
			.orElseThrow(() -> new RuntimeException("unsupport meta key: " + k)));
		this.metadataProjectIndices = metadataProjectIndices;
	}
}
