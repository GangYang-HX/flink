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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.bilibili.bsql.kafka.ability.metadata.read.ReadableMetadata;
import com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.*;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadMetadata;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProviderWithParallel;
import org.apache.flink.table.connector.source.SourceFunctionProviderWithExternalConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bilibili.bsql.common.global.SymbolsConstant;

import static com.bilibili.bsql.kafka.table.ParseUtil.*;
import static com.bilibili.bsql.kafka.util.KafkaTPDetector.getTopicTpNumber;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.*;


/**
 * Kafka {@link StreamTableSource} for Kafka 0.10.
 */
@Internal
public class BsqlKafka010DynamicSource implements ScanTableSource, SupportsReadMetadata, SupportsProjectionPushDown {

	private final static Logger LOG                          = LoggerFactory.getLogger(BsqlKafka010DynamicSource.class);

	private final static String KAFKA_CONSUMER_MODE_EARLIEST = "earliest";

	// --------------------------------------------------------------------------------------------
	// Common attributes
	// --------------------------------------------------------------------------------------------
	protected final DataType physicalDataType;

	// --------------------------------------------------------------------------------------------
	// Scan format attributes
	// --------------------------------------------------------------------------------------------

	/** Scan format for decoding records from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	// --------------------------------------------------------------------------------------------
	// Kafka-specific attributes
	// --------------------------------------------------------------------------------------------

	/** The Kafka topic to consume. */
	protected final String topic;

	/** Properties for the Kafka consumer. */
	protected final Properties properties;

	/** The Kafka source parallel. */
	protected final Integer sourceParallel;

	protected final Boolean topicIsPattern;

	protected final String offsetRest;

	protected final String tableName;

	protected int[] physicalProject;

	protected int[] metadataProject;

	protected List<String> metadataKeys;

	protected int[][] projectedFields;

	/**
	 * Creates a Kafka 0.10 {@link StreamTableSource}.
	 *
	 * @param physicalDataType       Source output data type
	 * @param topic                  Kafka topic to consume
	 * @param properties             Properties for the Kafka consumer
	 * @param decodingFormat         Decoding format for decoding records from Kafka
	 * @param topicIsPattern         Whether the topic is pattern
	 * @param offsetRest             Decide which kind of offset is
	 */
	public BsqlKafka010DynamicSource(
			DataType physicalDataType,
			String topic,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			Boolean topicIsPattern,
			String offsetRest,
			int sourceParallel,
			String tableName,
			int[] physicalProject) {

		this.physicalDataType = Preconditions.checkNotNull(
			physicalDataType, "physicalDataType data type must not be null.");
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.decodingFormat = Preconditions.checkNotNull(
			decodingFormat, "Decoding format must not be null.");
		this.offsetRest = Preconditions.checkNotNull(offsetRest, "offset reset should not be null");

		this.topicIsPattern = topicIsPattern;
		this.sourceParallel = sourceParallel;
		this.tableName = tableName;
		this.physicalProject = physicalProject;

		this.metadataKeys = Collections.emptyList();
		this.metadataProject = new int[0];
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return this.decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> valDeserializer =
			this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.physicalDataType);
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<RowData> kafkaConsumer =
			getKafkaConsumer(topic, properties, valDeserializer);

		if(topic.contains("*")){
			return SourceFunctionProviderWithParallel.of(kafkaConsumer, false, sourceParallel);
		} else {
			int tpNumber = getTopicTpNumber(properties, topic.split(SymbolsConstant.COMMA));
			return SourceFunctionProviderWithExternalConfig.of(kafkaConsumer, false, sourceParallel, tpNumber);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final BsqlKafka010DynamicSource that = (BsqlKafka010DynamicSource) o;
		return Objects.equals(physicalDataType, that.physicalDataType) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(decodingFormat, that.decodingFormat)&&
			Objects.equals(topicIsPattern, that.topicIsPattern)&&
			Objects.equals(offsetRest, that.offsetRest)&&
			Objects.equals(sourceParallel, that.sourceParallel);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			physicalDataType,
			topic,
			properties,
			decodingFormat,
			topicIsPattern,
			offsetRest,
			sourceParallel,
			tableName
			);
	}

	@Override
	public DynamicTableSource copy() {
		BsqlKafka010DynamicSource copy = new BsqlKafka010DynamicSource(
				this.physicalDataType,
				this.topic,
				this.properties,
				this.decodingFormat,
				this.topicIsPattern,
				this.offsetRest,
				this.sourceParallel,
				this.tableName,
				this.physicalProject
		);
		copy.metadataKeys = metadataKeys;
		copy.metadataProject = metadataProject;
		copy.physicalProject = physicalProject;
		copy.projectedFields = projectedFields;
		return copy;
	}

	protected FlinkKafkaConsumerBase<RowData> getKafkaConsumer(
		String topic,
		Properties properties,
		DeserializationSchema<RowData> valDeserialization) {
		ReadableMetadata.MetadataConverter[] converters = transform2Converter();
		KafkaDeserializationSchema<RowData> schemaWrapper = Boolean.parseBoolean(properties.getOrDefault(USE_LANCER_FORMAT.key(), false).toString()) ?
            new LancerDeserializationSchema(valDeserialization, physicalProject, metadataProject, getSelectedFields(),
					converters, properties.getProperty(LANCER_SINK_DEST.key())) :
            new DynamicKafkaDeserializationSchema(valDeserialization, physicalProject, metadataProject, getSelectedFields(), converters);

		FlinkKafkaConsumerBase<RowData> kafkaConsumer;
		if (topicIsPattern) {
			kafkaConsumer = new FlinkKafkaConsumer010<>(Pattern.compile(topic), schemaWrapper, properties);
		} else {
			List<String> topics = Arrays.stream(topic.split(",")).map(String::trim).collect(Collectors.toList());
			kafkaConsumer = new FlinkKafkaConsumer010<>(topics, schemaWrapper, properties);
		}

		if (KAFKA_CONSUMER_MODE_EARLIEST.equalsIgnoreCase(offsetRest)) {
			kafkaConsumer.setStartFromEarliest();
			LOG.info("set kafkaConsumer start from earliest");
		} else if (isJson(offsetRest)) {
			try {
				Properties offsetProperties = jsonStrToObject(offsetRest, Properties.class);
				Map<String, Object> offsetMap = ObjectToMap(offsetProperties);
				Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>(offsetMap.size());
				for (Map.Entry<String, Object> entry : offsetMap.entrySet()) {
					specificStartupOffsets.put(new KafkaTopicPartition(topic, Integer.parseInt(entry.getKey())),
						Long.valueOf(entry.getValue().toString()));
				}
				kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
			} catch (Exception e) {
				throw new RuntimeException("not support offsetReset type:" + offsetRest);
			}
		} else if (isTimeStamp(offsetRest)) {
			kafkaConsumer.setStartFromTimestamp(Long.parseLong(offsetRest));
			Date date = offsetRest.length() == 10 ?
				new Date(Long.parseLong(offsetRest) * 1000) :
				new Date(Long.parseLong(offsetRest));
			LOG.info("set kafkaConsumer start from target date:{}", date.toString());
		} else {
			kafkaConsumer.setStartFromLatest();
			LOG.info("set kafkaConsumer start from latest");
		}
		kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty("group.id") != null);
		kafkaConsumer.setMaxPartitionDiscoverRetries(Integer.parseInt(properties.getProperty(
			FlinkKafkaConsumerBase.FLINK_PARTITION_DISCOVERY_RETRIES)));
		return kafkaConsumer;
	}

	@Override
	public String asSummaryString() {
		return "Kafka-Source-" + tableName;
	}

	@Override
	public void applyReadMetadata(List<String> metadataKeys, int[] metadataProjectIndices) {
		this.metadataProject = metadataProjectIndices;
		this.metadataKeys = metadataKeys;
	}

	private ReadableMetadata.MetadataConverter[] transform2Converter() {
		if (metadataKeys == null) {
			return null;
		}
		return metadataKeys.stream()
				.map(
						k ->
								Stream.of(ReadableMetadata.values())
										.filter(rm -> rm.key.equals(k))
										.findFirst()
										.orElseThrow(() -> new RuntimeException("Unknown meta key: " + k)))
				.map(m -> m.converter)
				.toArray(ReadableMetadata.MetadataConverter[]::new);
	}

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.projectedFields = projectedFields;
	}

	private int[] getSelectedFields() {
		return projectedFields == null
				? IntStream.range(0, physicalProject.length + metadataProject.length).toArray()
				: Arrays.stream(projectedFields).mapToInt(array -> array[0]).toArray();
	}
}
