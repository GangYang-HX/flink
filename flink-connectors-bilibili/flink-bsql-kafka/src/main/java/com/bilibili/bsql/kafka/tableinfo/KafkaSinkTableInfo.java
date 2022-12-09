/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.tableinfo;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.bili.external.keeper.KeeperOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.factories.*;

import com.bilibili.bsql.common.SinkTableInfo;
import com.bilibili.bsql.common.format.BsqlDelimitFormatFactory;
import com.bilibili.bsql.kafka.sink.ModulePartitioner;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.*;
import lombok.Data;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @author zhouxiaogang
 * @version $Id: KafkaSinkTableInfo.java, v 0.1 2020-10-20 18:24
 * zhouxiaogang Exp $$
 */
@Data
public class KafkaSinkTableInfo extends SinkTableInfo {

	public static final Integer UNDEFINED_IDX = -1;

	private String bootstrapServers;
	private String topic;

	private Integer primaryKeyIndex;
	private String partitionStrategy;

	private Boolean kafkaRetry;

	public Map<String, String> kafkaParam;
	public transient EncodingFormat<SerializationSchema<RowData>> encodingFormat;
	public transient FlinkKafkaPartitioner<RowData> partitioner;
	private String serializer;

    private String traceId;
    private String traceKind;
    private String customTraceClass;

    private String blacklistEnable;
    private String blacklistZkHost;
    private String blacklistZkRootPath;
    private String blacklistKickThreshold;

    private Boolean isKafkaRecordOversizeIgnore;

	public KafkaSinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		this.topic = helper.getOptions().get(BSQL_TOPIC);
		this.bootstrapServers = helper.getOptions().get(BSQL_PROPS_BOOTSTRAP_SERVERS);
		//Not suitable for kafka diversion
		String connector = context.getCatalogTable().getOptions()
				.getOrDefault(ConnectorDescriptorValidator.CONNECTOR, "");
		if (StringUtils.isNotBlank(connector)
				&& !connector.equals("bsql-kafka10-diversion")
				&& StringUtils.isBlank(bootstrapServers)){
			String kafkaBrokerInfo = KeeperOperator.getKafkaBrokerInfo(topic);
			if (StringUtils.isNotBlank(kafkaBrokerInfo)){
				this.bootstrapServers = kafkaBrokerInfo;
			}
		}
		this.partitionStrategy = helper.getOptions().get(PARTITION_STRATEGY);
		this.kafkaRetry = helper.getOptions().get(BSQL_PROPS_KAFKA_RETRY);
		this.serializer = helper.getOptions().get(BSQL_PROPS_KAFKA_SERIALIZER);

		if (primaryKeyIdx.size() == 1) {
			this.primaryKeyIndex = primaryKeyIdx.get(0);
		} else {
			this.primaryKeyIndex = UNDEFINED_IDX;
		}

		this.kafkaParam = context.getCatalogTable().getOptions();

//		Configuration configuration = new Configuration();
//		context.getCatalogTable().getOptions().forEach(configuration::setString);
//		BsqlDelimitFormatFactory factory = new BsqlDelimitFormatFactory();
//		this.encodingFormat = factory.createEncodingFormat(context, configuration);

		this.encodingFormat = helper.discoverEncodingFormat(
			SerializationFormatFactory.class,
			BSQL_FORMAT);
		this.partitioner = getFlinkKafkaPartitioner(context.getClassLoader());
		checkArgument(StringUtils.isNotEmpty(topic));
		checkArgument(StringUtils.isNotEmpty(bootstrapServers));
		/**
		 * Hash Partitionor can not retried
		 * */
		if (org.apache.commons.lang.StringUtils.equalsIgnoreCase(
			ModulePartitioner.PARTITION_STRATEGY,
			partitionStrategy)
		){
			kafkaRetry = false;
		}

        this.traceId = helper.getOptions().get(SINK_TRACE_ID);
        this.traceKind = helper.getOptions().get(SINK_TRACE_KIND);
        this.customTraceClass = helper.getOptions().get(SINK_CUSTOM_TRACE_CLASS);

        this.blacklistEnable = helper.getOptions().get(BLACKLIST_ENABLE);
        this.blacklistZkHost = helper.getOptions().get(BLACKLIST_ZK_HOST);
        this.blacklistZkRootPath = helper.getOptions().get(BLACKLIST_ZK_ROOT_PATH);
        this.blacklistKickThreshold = helper.getOptions().get(BLACKLIST_KICK_THRESHOLD);

        this.isKafkaRecordOversizeIgnore = helper.getOptions().get(IS_KAFKA_OVER_SIZE_RECORD_IGNORE);
	}

	private FlinkKafkaPartitioner<RowData> getFlinkKafkaPartitioner(ClassLoader classLoader) {
		List<FlinkKafkaPartitioner<RowData>> foundPartitioner = new LinkedList<>();
		ServiceLoader
			.load(FlinkKafkaPartitioner.class,classLoader)
			.iterator()
			.forEachRemaining(foundPartitioner::add);
		List<FlinkKafkaPartitioner<RowData>> matchingPartitioner = foundPartitioner.stream().filter(p -> Objects.equals(p.partitionerIdentifier(), partitionStrategy)).collect(Collectors.toList());
		if (matchingPartitioner.isEmpty()) {
			throw new ValidationException(
				String.format(
					"Could not find any partitioner for partitionStrategy '%s' that implements '%s' in the classpath.\n\n" +
						"Available partitioner partitionStrategy are:\n\n" +
						"%s",
					partitionStrategy,
					FlinkKafkaPartitioner.class.getName(),
					foundPartitioner.stream()
						.map(FlinkKafkaPartitioner::partitionerIdentifier)
						.sorted()
						.collect(Collectors.joining("\n"))));
		}
		if (matchingPartitioner.size() > 1) {
			throw new ValidationException(
				String.format(
					"Multiple partitioner for partitionStrategy '%s' that implement '%s' found in the classpath.\n\n" +
						"Ambiguous partitioner classes are:\n\n" +
						"%s",
					partitionStrategy,
					FlinkKafkaPartitioner.class.getName(),
					foundPartitioner.stream()
						.map(f -> f.getClass().getName())
						.sorted()
						.collect(Collectors.joining("\n"))));
		}
		return matchingPartitioner.get(0);
	}
}
