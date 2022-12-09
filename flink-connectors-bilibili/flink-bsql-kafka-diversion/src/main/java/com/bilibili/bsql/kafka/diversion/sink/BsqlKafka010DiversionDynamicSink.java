package com.bilibili.bsql.kafka.diversion.sink;

import com.bilibili.bsql.common.format.CustomDelimiterSerialization;
import com.bilibili.bsql.kafka.diversion.producer.DiversionProducer;
import com.bilibili.bsql.kafka.diversion.producer.RetryDiversionProducer;
import com.bilibili.bsql.kafka.diversion.tableinfo.BsqlKafka010DiversionConfig;
import com.bilibili.bsql.kafka.diversion.tableinfo.KafkaDiversionSinkTableInfo;
import com.bilibili.bsql.kafka.diversion.wrapper.KafkaDiversionProtoBytesWrapper;
import com.bilibili.bsql.kafka.diversion.wrapper.KafkaDiversionWrapper;
import com.bilibili.bsql.kafka.sink.HashKeySerializationSchemaWrapper;
import com.bilibili.bsql.kafka.table.BsqlKafka010DynamicSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Properties;

public class BsqlKafka010DiversionDynamicSink extends BsqlKafka010DynamicSink {

	private final static Logger LOG = LoggerFactory.getLogger(BsqlKafka010DiversionDynamicSink.class);
	private final static String PROTO_BYTES = "proto_bytes";

	private final String topicUdfClassName;
	private final String broker;
	private final String brokerUdfClassName;
	private final String[] fieldNames;
	private final int cacheTtl;
	private final boolean excludeField;

	public BsqlKafka010DiversionDynamicSink(Optional<FlinkKafkaPartitioner<RowData>> partitioner,
											Properties properties,
											KafkaDiversionSinkTableInfo sinkTableInfo){
		this(sinkTableInfo.getNonGeneratedRowDataType(),
			sinkTableInfo.getPhysicalRowDataType(),
			sinkTableInfo.getTopic(),
			properties,
			partitioner,
			sinkTableInfo.getEncodingFormat(),
			sinkTableInfo.getParallelism(),
			sinkTableInfo.getPrimaryKeyIndex(),
			sinkTableInfo.getKafkaRetry(),
			sinkTableInfo.getTopicUdfClassName(),
			sinkTableInfo.getFieldNames(),
			sinkTableInfo.getSerializer(),
			sinkTableInfo.getBootstrapServers(),
			sinkTableInfo.getBrokerUdfClassName(),
			sinkTableInfo.getCacheTtl(),
			sinkTableInfo.getExcludeField());
	}

	public BsqlKafka010DiversionDynamicSink(DataType consumedDataType, DataType physicalDataType, String topic, Properties properties,
											Optional<FlinkKafkaPartitioner<RowData>> partitioner,
											EncodingFormat<SerializationSchema<RowData>> encodingFormat,
											Integer parallel, Integer primaryKeyIndex, Boolean kafkaFailRetry,
											String topicUdfClassName, String[] fieldNames, String serializer,
											String bootstrapServers, String brokerUdfClassName, String cacheTtl,
											boolean excludeField) {
		super(consumedDataType, physicalDataType, topic, properties, partitioner, encodingFormat, parallel, primaryKeyIndex, kafkaFailRetry, serializer);
		this.topicUdfClassName = topicUdfClassName;
		this.broker = bootstrapServers;
		this.brokerUdfClassName = brokerUdfClassName;
		this.fieldNames = fieldNames;
		this.cacheTtl = Integer.parseInt(cacheTtl);
		this.excludeField = excludeField;
	}


	@Override
	public SinkFunction<RowData> createKafkaProducer(HashKeySerializationSchemaWrapper schemaWrapper) {
		final SinkFunction<RowData> kafkaProducer;
		if (this.kafkaFailRetry) {
			kafkaProducer = new RetryDiversionProducer(
				this.topic,
				this.broker,
				this.brokerUdfClassName,
				schemaWrapper,
				this.properties,
				this.partitioner.orElse(null),
				1
			);
		} else{
			kafkaProducer = new DiversionProducer(
				this.topic,
				this.broker,
				this.brokerUdfClassName,
				schemaWrapper,
				this.properties,
				this.partitioner.orElse(null));
		}
		return kafkaProducer;
	}

	@Override
	public HashKeySerializationSchemaWrapper generateSchemaWrapper(DynamicTableSink.Context context) {
		DynamicTableSink.DataStructureConverter converter = context.createDataStructureConverter(consumedDataType);
		SerializationSchema<RowData> serializationSchema = this.encodingFormat.createRuntimeEncoder(context, this.physicalDataType);

		//valid udf class
		Class<?> topicUdfClass = getAndVaildUdf(topicUdfClassName, topic);
		Class<?> brokerUdfClass = getAndVaildUdf(brokerUdfClassName, broker);
		if (topicUdfClass == null && brokerUdfClass == null) {
			throw new IllegalArgumentException(String.format("You need to specify the udf class full name with '%s' or " +
				"'%s'", BsqlKafka010DiversionConfig.BSQL_TOPIC_UDF.key(), BsqlKafka010DiversionConfig.BSQL_BROKER_UDF.key()));
		}
		int[] topicFieldIndex = new int[0];
		int[] brokerFieldIndex = new int[0];
		//valid method
		if (topicUdfClass != null) {
			validMethod(topicUdfClass, topic);
			//find columns index
			topicFieldIndex = findFieldIndex(topic);
		}
		if (brokerUdfClass != null) {
			validMethod(brokerUdfClass, broker);
			//find columns index
			brokerFieldIndex = findFieldIndex(broker);
		}

		if (PROTO_BYTES.equals(this.serializer)
			&& serializationSchema instanceof PbRowDataSerializationSchema) {
			return new KafkaDiversionProtoBytesWrapper((PbRowDataSerializationSchema)serializationSchema, primaryKeyIndex, converter,
				topicFieldIndex, topicUdfClass, brokerFieldIndex, brokerUdfClass, cacheTtl,
				excludeField, metadataProjectIndices, metadataKeys);
		}

		return new KafkaDiversionWrapper((CustomDelimiterSerialization) serializationSchema, primaryKeyIndex, converter,
			topicFieldIndex, topicUdfClass, brokerFieldIndex, brokerUdfClass, cacheTtl,
			excludeField, metadataProjectIndices, metadataKeys);
	}

//	private Class<?> getAndVaildUdf(String udfClassName, String udfProperty) {
//		String propertyClassName = udfProperty.split("\\(")[0];
//		if (StringUtils.isNullOrWhitespaceOnly(udfClassName)) {
//			return null;
//		} else {
//			if (propertyClassName.equals(udfClassName.substring(udfClassName.lastIndexOf(".") + 1))) {
//				try {
//					return Class.forName(udfClassName);
//				} catch (ClassNotFoundException e) {
//					throw new IllegalArgumentException("can not find udf class named '" + udfClassName + "'");
//				}
//			} else {
//				throw new IllegalArgumentException("You need to specify the udf class full name for " + propertyClassName);
//			}
//		}
//	}

	private Class<?> getAndVaildUdf(String udfClassName, String udfProperty) {
		String propertyClassName = udfProperty.split("\\(")[0];
		if (StringUtils.isNullOrWhitespaceOnly(udfClassName)) {
			return null;
		} else {
			if (propertyClassName.equals(udfClassName.substring(udfClassName.lastIndexOf(".") + 1))) {
				return getUdf(udfClassName);
			} else {
				throw new IllegalArgumentException("You need to specify the udf class full name for " + propertyClassName);
			}
		}
	}

	public  Class<?> getUdf(String udfClassName) throws IllegalArgumentException {
		ClassLoader dynamicSinkClassLoader = this.getClass().getClassLoader();
		ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
		LOG.info("when sink kafka,ClassLoader load info,dynamicSinkClassLoader={},currentThreadClassLoader={},udfClassName={}"
			, dynamicSinkClassLoader.toString(), currentThreadClassLoader.toString(), udfClassName);
		try {
			return Class.forName(udfClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("when sink kafka for name can not find udf class named '" + udfClassName + "'");
		}
		try {
			return currentThreadClassLoader.loadClass(udfClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("when sink kafka threadUdfLoadClass can not find udf class named '" + udfClassName + "'");
		}
		try {
			return dynamicSinkClassLoader.loadClass(udfClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("when sink kafka dynamicSinkClassLoader can not find udf class named '" + udfClassName + "'");
		}
		throw new IllegalArgumentException("when sink kafka can not find udf class named '" + udfClassName + "'");
	}

	private void validMethod(Class<?> udfClass, String udfProperty) {
		int evalMethodCount = 0;
		Method evalMethod = null;
		for (Method method : udfClass.getMethods()) {
			if ("eval".equals(method.getName())) {
				evalMethod = method;
				evalMethodCount++;
			}
		}
		if (evalMethodCount != 1) {
			throw new IllegalArgumentException("the " + udfClass + " class don't have unique eval method");
		}
		if (evalMethod.getParameterCount() != udfProperty.split("\\(")[1].replaceAll("\\)", "").trim().split(",").length) {
			throw new IllegalArgumentException(udfClass.getSimpleName() + ": The number of parameters does not match.");
		}
	}

	private int[] findFieldIndex(String udfProperty) {
		int[] filedIndex;
		String[] inputColumns = udfProperty.split("\\(")[1].replaceAll("\\)", "").trim().split(",");
		filedIndex = new int[inputColumns.length];
		for (int i = 0; i < inputColumns.length; i++) {
			for (int j = 0; j < fieldNames.length; j++) {
				if (inputColumns[i].trim().equals(fieldNames[j])) {
					filedIndex[i] = j;
					LOG.info("input column:{}, field index:{}", inputColumns[i].trim(), j);
				}
			}
		}
		return filedIndex;
	}

	@Override
	public DynamicTableSink copy() {
		return new BsqlKafka010DiversionDynamicSink(
			this.consumedDataType,
			this.physicalDataType,
			this.topic,
			this.properties,
			this.partitioner,
			this.encodingFormat,
			this.parallel,
			this.primaryKeyIndex,
			this.kafkaFailRetry,
			this.topicUdfClassName,
			this.fieldNames,
			this.serializer,
			this.broker,
			this.brokerUdfClassName,
			String.valueOf(this.cacheTtl),
			this.excludeField
		);
	}

	@Override
	public String asSummaryString() {
		return "Kafka 0.10 diversion table sink";
	}
}
