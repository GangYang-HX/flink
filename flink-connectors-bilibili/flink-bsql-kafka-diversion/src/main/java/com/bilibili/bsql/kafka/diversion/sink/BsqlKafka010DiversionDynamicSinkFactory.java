package com.bilibili.bsql.kafka.diversion.sink;

import com.bilibili.bsql.common.format.BsqlDelimitFormatFactory;
import com.bilibili.bsql.kafka.diversion.tableinfo.KafkaDiversionSinkTableInfo;
import com.bilibili.bsql.kafka.sink.ModulePartitioner;
import com.bilibili.bsql.kafka.table.BsqlKafka010DynamicSinkFactory;
import com.bilibili.bsql.kafka.tableinfo.KafkaSinkTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.bilibili.bsql.kafka.diversion.tableinfo.BsqlKafka010DiversionConfig.BSQL_TOPIC_UDF;
import static com.bilibili.bsql.kafka.tableinfo.BsqlKafka010Config.getKafkaProperties;

public class BsqlKafka010DiversionDynamicSinkFactory extends BsqlKafka010DynamicSinkFactory {

	public static final String IDENTIFIER = "bsql-kafka10-diversion";

    private static final String BLACKLIST_ENABLE = "blacklist.enable";

    private static final String BLACKLIST_ZK_HOST = "blacklist.zk.host";

    private static final String BLACKLIST_ZK_ROOT_PATH = "blacklist.zk.root.path";

    private static final String BLACKLIST_KICK_THRESHOLD = "blacklist.kick.threshold";

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public KafkaSinkTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context){
		/**
		 * this is to obey the open source rule, make sure when added "format", the parse logic won't change
		 */
		context.getCatalogTable().getOptions().put("format", BsqlDelimitFormatFactory.IDENTIFIER);
		return new KafkaDiversionSinkTableInfo(helper, context);
	}


	@Override
	public DynamicTableSink generateTableSink(KafkaSinkTableInfo kafkaSinkTableInfo) {
		KafkaDiversionSinkTableInfo kafkaDiversionSinkTableInfo = (KafkaDiversionSinkTableInfo) kafkaSinkTableInfo;
		Optional<FlinkKafkaPartitioner<RowData>> partitioner;
		if (StringUtils.equalsIgnoreCase(ModulePartitioner.PARTITION_STRATEGY, kafkaDiversionSinkTableInfo.getPartitionStrategy())) {
			partitioner = Optional.of(new ModulePartitioner());
		} else {
			partitioner = Optional.of(new FlinkFixedPartitioner());
		}

		Properties properties = getKafkaProperties(kafkaDiversionSinkTableInfo.getKafkaParam());
		properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "saber_producer_" + kafkaDiversionSinkTableInfo.getJobId());
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDiversionSinkTableInfo.getBootstrapServers());
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
		if (!properties.containsKey(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) {
			properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
		}

        properties.put(BLACKLIST_ENABLE, kafkaSinkTableInfo.getBlacklistEnable());
        properties.put(BLACKLIST_ZK_HOST, kafkaSinkTableInfo.getBlacklistZkHost());
        properties.put(BLACKLIST_ZK_ROOT_PATH, kafkaSinkTableInfo.getBlacklistZkRootPath());
        properties.put(BLACKLIST_KICK_THRESHOLD, kafkaSinkTableInfo.getBlacklistKickThreshold());

		return new BsqlKafka010DiversionDynamicSink(partitioner, properties, kafkaDiversionSinkTableInfo);
	}
	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> basicOption) {
		super.setRequiredOptions(basicOption);
		basicOption.add(BSQL_TOPIC_UDF);
	}
}
