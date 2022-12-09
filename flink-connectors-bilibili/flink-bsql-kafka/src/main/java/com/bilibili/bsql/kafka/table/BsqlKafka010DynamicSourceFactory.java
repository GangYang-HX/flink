/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSourceFactory;
import com.bilibili.bsql.kafka.tableinfo.KafkaSourceTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.Set;

import static com.bilibili.bsql.kafka.table.ParseUtil.isJson;
import static com.bilibili.bsql.kafka.table.ParseUtil.isTimeStamp;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.*;


/**
 *
 * @author zhouxiaogang
 * @version $Id: bsqlKafkaDynamicTableFactory.java, v 0.1 2020-09-27 16:29
zhouxiaogang Exp $$
 */
public class BsqlKafka010DynamicSourceFactory extends BsqlDynamicTableSourceFactory<KafkaSourceTableInfo> {

	public static final String IDENTIFIER = "bsql-kafka10";

	@Override
	public KafkaSourceTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context){
		/**
		 * this is to obey the open source rule, make sure when added "format", the parse logic won't change
		 */
		return new KafkaSourceTableInfo(helper, context);
	}


	@Override
	public DynamicTableSource generateTableSource(KafkaSourceTableInfo sourceTableInfo) {
		/**
		 * kafka parser logic
		 * 1, get the parallel -- ok
		 * 2, get bootstrap server -- ok
		 * 3, get topic -- ok
		 * 4, get offset -- ok
		 * 5, get topic pattern -- ok
		 * 6, get the time zone -- maybe not necessary now
		 * 7, get the property with prefix of "kafka"
		 * -- maybe not necessary now, saber use kafka.xxx, need to change to property
		 * 8, get the delimit option -- ok
		 *
		 * 9, ratelimiter -- maybe not necessary now
		 */

		String topic = sourceTableInfo.getTopic();
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = sourceTableInfo.getDecodingFormat();
		// Validate the option data type.
//		helper.validateExcept(PROPERTIES_PREFIX);
		// Validate the option values.
//		validateBsqlTableOptions(tableOptions);

		DataType physicalDataType = sourceTableInfo.getPhysicalRowDataType();
		int[] physicalProjectIndices = sourceTableInfo.createPhysicalProjectIndex();
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE.toString());
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceTableInfo.getBootstrapServers());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "saber_" + sourceTableInfo.getJobId());
		props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "saber_consumer_" + sourceTableInfo.getJobId());
		if (isTimeStamp(sourceTableInfo.getOffsetReset())) {
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, sourceTableInfo.getDefOffsetReset());
		}else {
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, sourceTableInfo.getOffsetReset());
		}
		Properties propsSetting = getKafkaProperties(sourceTableInfo.getKafkaParam());
		for (String propertyKey : propsSetting.stringPropertyNames()) {
			props.setProperty(propertyKey, propsSetting.getProperty(propertyKey));
		}

		// 开启分区变化自动发现,2min探测一次
		props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "120000");
		props.setProperty(FlinkKafkaConsumerBase.FLINK_PARTITION_DISCOVERY_RETRIES, sourceTableInfo.getPartitionDiscoverRetries());
		props.setProperty(CONSUMER_DETECT_REQUEST_TIMEOUT_MS.key(), sourceTableInfo.getDetectRequestTimeoutMs());

        props.setProperty(USE_LANCER_FORMAT.key(), String.valueOf(sourceTableInfo.isUseLancerFormat()));
        if (sourceTableInfo.isUseLancerFormat()){
            props.setProperty(LANCER_SINK_DEST.key(), String.valueOf(sourceTableInfo.getLancerSinkDest()));
            props.setProperty(SINK_TRACE_ID.key(), String.valueOf(sourceTableInfo.getTraceId()));
        }


        props.put(BLACKLIST_ENABLE.key(), sourceTableInfo.getBlacklistEnable());
        props.put(BLACKLIST_ZK_HOST.key(), sourceTableInfo.getBlacklistZkHost());
        props.put(BLACKLIST_ZK_ROOT_PATH.key(), sourceTableInfo.getBlacklistZkRootPath());
        props.put(BLACKLIST_LAG_THRESHOLD.key(), sourceTableInfo.getBlacklistLagThreshold());
        props.put(BLACKLIST_KICK_THRESHOLD.key(), sourceTableInfo.getBlacklistKickThreshold());
        props.put(BLACKLIST_LAG_TIMES.key(), sourceTableInfo.getBlacklistLagTimes());

		return new BsqlKafka010DynamicSource(
			physicalDataType,
			topic,
			props,
			decodingFormat,
			sourceTableInfo.getTopicIsPattern(),
			sourceTableInfo.getOffsetReset(),
			sourceTableInfo.getParallelism(),
			sourceTableInfo.getName(),
			physicalProjectIndices
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
//		basicOption.add(DELIMITER_KEY);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> basicOption) {
		/** to inheret the saber ddl rule
		 * https://info.bilibili.co/pages/viewpage.action?pageId=64962724#DDL%E8%AF%AD%E6%B3%95-1)kafka
		 * do not use the open source, add a offset option
		 */
		basicOption.add(BSQL_GROUP_OFFSETS);
		basicOption.add(BSQL_FORMAT);
		basicOption.add(TOPIC_IS_PATTERN);
		basicOption.add(BSQL_DEFAULT_OFFSET_RESET);
//		options.add(SCAN_STARTUP_MODE);
//		options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
//		options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
//		options.add(SINK_PARTITIONER);
	}

}
