/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSinkFactory;
import com.bilibili.bsql.common.format.BsqlDelimitFormatFactory;
import com.bilibili.bsql.kafka.sink.ModulePartitioner;
import com.bilibili.bsql.kafka.sink.WellDistributedPartitioner;
import com.bilibili.bsql.kafka.tableinfo.KafkaSinkTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.*;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BsqlKafka010DynamicSinkFactory.java, v 0.1 2020-10-13 16:52
zhouxiaogang Exp $$
 */
public class BsqlKafka010DynamicSinkFactory extends BsqlDynamicTableSinkFactory<KafkaSinkTableInfo> {

	public static final String IDENTIFIER = "bsql-kafka10";

	private static final String TRACE_ID = "traceId";

	private static final String TRACE_KIND = "traceKind";

	private static final String CUSTOM_TRACE_CLASS = "custom.trace.class";

    private static final String BLACKLIST_ENABLE = "blacklist.enable";

    private static final String BLACKLIST_ZK_HOST = "blacklist.zk.host";

    private static final String BLACKLIST_ZK_ROOT_PATH = "blacklist.zk.root.path";

    private static final String BLACKLIST_KICK_THRESHOLD = "blacklist.kick.threshold";

    private static final String IS_KAFKA_OVER_SIZE_RECORD_IGNORE = "isKafkaRecordOversizeIgnore";


	@Override
	public KafkaSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context){
		/**
		 * this is to obey the open source rule, make sure when added "format", the parse logic won't change
		 */
		context.getCatalogTable().getOptions().putIfAbsent("format", BsqlDelimitFormatFactory.IDENTIFIER);
		return new KafkaSinkTableInfo(helper, context);
	}

	@Override
	public DynamicTableSink generateTableSink(KafkaSinkTableInfo kafkaSinkTableInfo) {
		String topic = kafkaSinkTableInfo.getTopic();
		EncodingFormat<SerializationSchema<RowData>> encodingFormat = kafkaSinkTableInfo.getEncodingFormat();
		// Validate the option data type.
//		helper.validateExcept(PROPERTIES_PREFIX);
//		// Validate the option values.
//		validateBsqlTableOptions(tableOptions);
		DataType producedDataType = kafkaSinkTableInfo.getNonGeneratedRowDataType();
		DataType physicalDataType = kafkaSinkTableInfo.getPhysicalRowDataType();
		Optional<FlinkKafkaPartitioner<RowData>> partitioner = Optional.of(kafkaSinkTableInfo.getPartitioner());
		Properties properties = getKafkaProperties(kafkaSinkTableInfo.getKafkaParam());
		properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "saber_producer_" + kafkaSinkTableInfo.getJobId());
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkTableInfo.getBootstrapServers());
		if (!properties.containsKey(ProducerConfig.RETRIES_CONFIG)) {
			properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
		}
		if (!properties.containsKey(ProducerConfig.RETRY_BACKOFF_MS_CONFIG)) {
			properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "6000");
		}
		if (!properties.containsKey(ProducerConfig.BATCH_SIZE_CONFIG)) {
			properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "262144");
		}
		if (!properties.containsKey(ProducerConfig.ACKS_CONFIG)) {
			properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
		}
		if (!properties.containsKey(ProducerConfig.BUFFER_MEMORY_CONFIG)) {
			properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "63554432");
		}
		if (!properties.containsKey(ProducerConfig.LINGER_MS_CONFIG)) {
			properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");
		}
		if (!properties.containsKey(ProducerConfig.COMPRESSION_TYPE_CONFIG)) {
			properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
		}
		if (!properties.containsKey(ProducerConfig.MAX_REQUEST_SIZE_CONFIG)) {
			//set default value: 5MB
			properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "5242880");
		}

        properties.put(TRACE_ID, kafkaSinkTableInfo.getTraceId());
        properties.put(TRACE_KIND, kafkaSinkTableInfo.getTraceKind());
        properties.put(CUSTOM_TRACE_CLASS, kafkaSinkTableInfo.getCustomTraceClass());

        properties.put(BLACKLIST_ENABLE, kafkaSinkTableInfo.getBlacklistEnable());
        properties.put(BLACKLIST_ZK_HOST, kafkaSinkTableInfo.getBlacklistZkHost());
        properties.put(BLACKLIST_ZK_ROOT_PATH, kafkaSinkTableInfo.getBlacklistZkRootPath());
        properties.put(BLACKLIST_KICK_THRESHOLD, kafkaSinkTableInfo.getBlacklistKickThreshold());

        properties.put(IS_KAFKA_OVER_SIZE_RECORD_IGNORE, kafkaSinkTableInfo.getIsKafkaRecordOversizeIgnore());

		return new BsqlKafka010DynamicSink(
			producedDataType,
			physicalDataType,
			topic,
			properties,
			partitioner,
			encodingFormat,
			kafkaSinkTableInfo.getParallelism(),
			kafkaSinkTableInfo.getPrimaryKeyIndex(),
			kafkaSinkTableInfo.getKafkaRetry(),
			kafkaSinkTableInfo.getSerializer()
		);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> basicOption) {
		// we don't need to parse the format as we only need delimit deserlialization
		// so remove the format as the
		basicOption.add(BSQL_TOPIC);
		basicOption.add(BSQL_PROPS_BOOTSTRAP_SERVERS);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> basicOption) {
		basicOption.add(PARTITION_STRATEGY);
	}

}
