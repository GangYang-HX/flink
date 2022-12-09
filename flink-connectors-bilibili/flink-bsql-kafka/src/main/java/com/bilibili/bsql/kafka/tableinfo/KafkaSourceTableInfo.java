/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.tableinfo;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.bili.external.keeper.KeeperOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.bilibili.bsql.common.SourceTableInfo;
import com.bilibili.bsql.common.format.BsqlDelimitFormatFactory;

import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.*;
import static com.bilibili.bsql.kafka.table.ParseUtil.isTimeStamp;
import lombok.Data;

/**
 *
 * @author zhouxiaogang
 * @version $Id: KafkaSourceTableInfo.java, v 0.1 2020-10-13 17:09
zhouxiaogang Exp $$
 */
@Data
public class KafkaSourceTableInfo extends SourceTableInfo {
	private String             bootstrapServers;
	private String             topic;
	private String             offset;
	private String             offsetReset;
	private String             defOffsetReset;
	private Boolean            topicIsPattern;
//	private String             delimiterKey;
	public Map<String, String> kafkaParam;

	private String partitionDiscoverRetries;
	private String detectRequestTimeoutMs;

    private String blacklistEnable;
    private String blacklistZkHost;
    private String blacklistZkRootPath;
    private String blacklistLagThreshold;
    private String blacklistKickThreshold;
    private String blacklistLagTimes;
    private String blacklistProducerKickThreshold;

    private boolean useLancerFormat;
    private String lancerSinkDest;
    private String traceId;

	public transient DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	public KafkaSourceTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		this.topic = helper.getOptions().get(BSQL_TOPIC);
		this.bootstrapServers = helper.getOptions().get(BSQL_PROPS_BOOTSTRAP_SERVERS);

		this.offset = helper.getOptions().get(BSQL_GROUP_OFFSETS);
		this.offsetReset = helper.getOptions().get(BSQL_GROUP_OFFSETS);
		this.defOffsetReset = helper.getOptions().get(BSQL_DEFAULT_OFFSET_RESET);
		this.topicIsPattern = helper.getOptions().get(TOPIC_IS_PATTERN);
		if (!topicIsPattern && StringUtils.isBlank(bootstrapServers)){
			String bootstrapInfo = KeeperOperator.getKafkaBrokerInfo(topic);
			if (StringUtils.isNotBlank(bootstrapInfo)){
				this.bootstrapServers = bootstrapInfo;
			}
		}
		/**
		 * in saber the time zone key is useless
		 * */

		this.kafkaParam = context.getCatalogTable().getOptions();
		this.partitionDiscoverRetries = helper.getOptions().get(PARTITION_DISCOVER_RETRY);
		this.detectRequestTimeoutMs = helper.getOptions().get(CONSUMER_DETECT_REQUEST_TIMEOUT_MS);


//		Configuration configuration = new Configuration();
//		context.getCatalogTable().getOptions().forEach(configuration::setString);
//		BsqlDelimitFormatFactory factory = new BsqlDelimitFormatFactory();
//		this.decodingFormat = factory.createDecodingFormat(context, configuration);

		this.decodingFormat =	helper.discoverDecodingFormat(
			DeserializationFormatFactory.class,
			BSQL_FORMAT);

        this.useLancerFormat = helper.getOptions().get(USE_LANCER_FORMAT);
        this.lancerSinkDest = helper.getOptions().get(LANCER_SINK_DEST);
        this.traceId = helper.getOptions().get(SINK_TRACE_ID);

		if (StringUtils.isEmpty(topic)) {
			throw new IllegalArgumentException("source表:" + getName() + "没有填写topic属性");
		}
		if (StringUtils.isEmpty(bootstrapServers)) {
			throw new IllegalArgumentException("source表:" + getName() + "没有填写bootstrapServers属性");
		}
		if (StringUtils.isEmpty(offsetReset)) {
			throw new IllegalArgumentException("source表:" + getName() + "没有填写offsetReset属性");
		}

        this.blacklistEnable = helper.getOptions().get(BLACKLIST_ENABLE);
        this.blacklistZkHost = helper.getOptions().get(BLACKLIST_ZK_HOST);
        this.blacklistZkRootPath = helper.getOptions().get(BLACKLIST_ZK_ROOT_PATH);
        this.blacklistLagThreshold = helper.getOptions().get(BLACKLIST_LAG_THRESHOLD);
        this.blacklistKickThreshold = helper.getOptions().get(BLACKLIST_KICK_THRESHOLD);
        this.blacklistLagTimes = helper.getOptions().get(BLACKLIST_LAG_TIMES);
		if (isTimeStamp(offsetReset) && StringUtils.isEmpty(defOffsetReset)) {
			throw new IllegalArgumentException("source表：" + getName() + "指定13位时间戳消费必须同时指定默认恢复策略，" +
				"即指定配置项'defOffsetReset为latest或earliest.");
		}
	}
}
