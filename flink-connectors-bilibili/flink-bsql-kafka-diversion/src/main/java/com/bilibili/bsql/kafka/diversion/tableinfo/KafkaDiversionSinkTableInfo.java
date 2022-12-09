package com.bilibili.bsql.kafka.diversion.tableinfo;

import com.bilibili.bsql.kafka.tableinfo.KafkaSinkTableInfo;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.kafka.diversion.tableinfo.BsqlKafka010DiversionConfig.*;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.BLACKLIST_ENABLE;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.BLACKLIST_ZK_HOST;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.BLACKLIST_ZK_ROOT_PATH;

public class KafkaDiversionSinkTableInfo extends KafkaSinkTableInfo {

	private final String topicUdfClassName;
	private final String brokerUdfClassName;
	private final String cacheTtl;
	private final boolean excludeField;

	private String blacklistEnable;
	private String blacklistZkHost;
	private String blacklistZkRootPath;

	public KafkaDiversionSinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		this.topicUdfClassName = helper.getOptions().get(BSQL_TOPIC_UDF);
		this.brokerUdfClassName = helper.getOptions().get(BSQL_BROKER_UDF);
		this.cacheTtl = helper.getOptions().get(CACHE_TTL);
		this.excludeField = helper.getOptions().get(EXCLUDE_ARG_FIELD);

		this.blacklistEnable = helper.getOptions().get(BLACKLIST_ENABLE);
		this.blacklistZkHost = helper.getOptions().get(BLACKLIST_ZK_HOST);
		this.blacklistZkRootPath = helper.getOptions().get(BLACKLIST_ZK_ROOT_PATH);
	}

	public String getTopicUdfClassName() {
		return topicUdfClassName;
	}

	public String getBrokerUdfClassName() {
		return brokerUdfClassName;
	}

	public String getCacheTtl() {
		return cacheTtl;
	}

	public boolean getExcludeField() {
		return excludeField;
	}
}
