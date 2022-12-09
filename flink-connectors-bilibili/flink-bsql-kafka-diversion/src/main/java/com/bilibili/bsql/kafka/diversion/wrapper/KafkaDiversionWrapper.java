package com.bilibili.bsql.kafka.diversion.wrapper;

import com.bilibili.bsql.common.format.CustomDelimiterSerialization;
import com.bilibili.bsql.kafka.diversion.cache.GuavaCacheFactory;
import com.bilibili.bsql.kafka.diversion.cache.KeyUtil;
import com.bilibili.bsql.kafka.sink.HashKeySerializationSchemaWrapper;
import com.google.common.cache.Cache;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.bilibili.bsql.common.utils.ObjectUtil.getLocalDateTimestamp;

public class KafkaDiversionWrapper extends HashKeySerializationSchemaWrapper {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaDiversionWrapper.class);

	private final int[] topicFieldIndices;
	private final Class<?> topicUdfClass;
	private final int[] brokerFieldIndices;
	private final Class<?> brokerUdfClass;
	private UserDefinedFunction topicUdfInstance;
	private UserDefinedFunction brokerUdfInstance;
	// Method object can't be serialized.
	private transient Method topicEvalMethod;
	private transient Method brokerEvalMethod;
	private final int cacheTtl;
	private final boolean excludeField;

	//cache
	private Cache<String, String> topicUdfCache;

	private Cache<String, String> brokerUdfCache;

	public KafkaDiversionWrapper(CustomDelimiterSerialization serializationSchema, 
								 int keyIndex,
								 DynamicTableSink.DataStructureConverter converter,
								 int[] topicFieldIndices,
								 Class<?> topicUdfClass,
								 int[] brokerFieldIndices,
								 Class<?> brokerUdfClass,
								 int cacheTtl,
								 boolean excludeField,
								 int[] metaColumnIndices,
								 List<String> metadataKeys) {
		super(serializationSchema, keyIndex, converter, metaColumnIndices, metadataKeys);
		this.topicFieldIndices = topicFieldIndices;
		this.topicUdfClass = topicUdfClass;
		this.brokerFieldIndices = brokerFieldIndices;
		this.brokerUdfClass = brokerUdfClass;
		this.cacheTtl = cacheTtl;
		this.excludeField = excludeField;
		init();
	}

	public KafkaDiversionWrapper(SerializationSchema<RowData> serializationSchema,
								 int keyIndex,
								 DynamicTableSink.DataStructureConverter converter,
								 int[] topicFieldIndices,
								 Class<?> topicUdfClass,
								 int[] brokerFieldIndices,
								 Class<?> brokerUdfClass,
								 int cacheTtl,
								 boolean excludeField,
								 int[] metaColumnIndices,
								 List<String> metadataKeys) {
		super(serializationSchema, keyIndex, converter, metaColumnIndices, metadataKeys);
		this.topicFieldIndices = topicFieldIndices;
		this.topicUdfClass = topicUdfClass;
		this.brokerFieldIndices = brokerFieldIndices;
		this.brokerUdfClass = brokerUdfClass;
		this.cacheTtl = cacheTtl;
		this.excludeField = excludeField;
		init();
	}

	private void init() {
		if (topicUdfClass != null) {
			topicUdfInstance = initInstance(topicUdfClass);
			topicUdfCache = new GuavaCacheFactory<String, String>().getCache(cacheTtl);
		}
		if (brokerUdfClass != null) {
			brokerUdfInstance = initInstance(brokerUdfClass);
			brokerUdfCache = new GuavaCacheFactory<String, String>().getCache(cacheTtl);
		}
	}

	@Override
	public String getTargetTopic(RowData element) {
		if (topicUdfClass == null) {
			return null;
		}
		if (topicEvalMethod == null) {
			topicEvalMethod = initMethod(topicUdfClass);
		}
		String targetTopic = null;
		Row row = (Row) getConverter().toExternal(element);
		try {
			Object[] args = new Object[topicFieldIndices.length];
			for (int i = 0; i < args.length; i++) {
				args[i] = row.getField(topicFieldIndices[i]);
			}
			String key = KeyUtil.transformKey(args);
			if (KeyUtil.UNCACHE_KEY.equals(key)) {
				//no cache
				LOG.warn("key: {} don't cache", key);
				targetTopic = (String) topicEvalMethod.invoke(topicUdfInstance, args);
			} else {
				targetTopic = topicUdfCache.get(key, () -> (String) topicEvalMethod.invoke(topicUdfInstance, args));
			}
		} catch (Exception e) {
			LOG.error("topic udf call error, detail: {}", getErrorMessage(row, topicFieldIndices), e);
		}
		if (StringUtils.isBlank(targetTopic)) {
			throw new RuntimeException("UDF获取 Topic 异常!");
		}
		return targetTopic;
	}

	@Override
	public String getTargetBrokers(RowData element) {
		if (brokerUdfClass == null) {
			return null;
		}
		if (brokerEvalMethod == null) {
			brokerEvalMethod = initMethod(brokerUdfClass);
		}
		String brokers = null;
		Row row = (Row) getConverter().toExternal(element);
		try {
			Object[] args = new Object[brokerFieldIndices.length];
			for (int i = 0; i < args.length; i++) {
				args[i] = row.getField(brokerFieldIndices[i]);
			}
			String key = KeyUtil.transformKey(args);
			if (KeyUtil.UNCACHE_KEY.equals(key)) {
				//don't cache
				LOG.warn("key: {} don't cache", key);
				brokers = (String) brokerEvalMethod.invoke(brokerUdfInstance, args);
			} else {
				brokers = brokerUdfCache.get(key, () -> (String) brokerEvalMethod.invoke(brokerUdfInstance, args));
			}
		} catch (Exception e) {
			LOG.error("bootstrapServers udf call error, detail: {}", getErrorMessage(row, brokerFieldIndices), e);
		}
		if (StringUtils.isBlank(brokers)) {
			throw new RuntimeException("UDF获取 brokers 地址异常!");
		}
		return brokers;
	}

	private UserDefinedFunction initInstance(Class<?> udfClass) {
		try {
			UserDefinedFunction udfInstance = (UserDefinedFunction) udfClass.newInstance();
			udfInstance.open(null);
			return udfInstance;
		} catch (Exception e) {
			throw new RuntimeException("create instance error.");
		}
	}

	private Method initMethod(Class<?> udfClass) {
		Method udfMethod = null;
		for (Method method : udfClass.getMethods()) {
			if ("eval".equals(method.getName())) {
				udfMethod = method;
				break;
			}
		}
		return udfMethod;
	}

	private String getErrorMessage(Row row, int[] FieldIndices) {
		StringBuilder msg = new StringBuilder();
		if (row != null) {
			Arrays.stream(FieldIndices).forEach(i -> {
				Object fieldValue = row.getField(i);
				if (fieldValue != null) {
					msg.append(fieldValue).append(",");
				} else {
					msg.append("null").append(",");
				}
			});
		}
		return msg.toString();
	}

	@Override
	protected byte[] serializeValue1(Row row) {
		final String[] fieldNames = ((RowDataTypeInfo) typeInfo).getFieldNames();
		if (row.getArity() != fieldNames.length) {
			throw new IllegalStateException(
				String.format("Number of elements in the row '%s' is different from number of field names: %d",
					row, fieldNames.length));
		}
		try {
			List<String> data = new ArrayList<>();
			for (int i = 0; i < fieldNames.length; i++) {
				if (excludeField && (findEleExist(topicFieldIndices, i) || findEleExist(brokerFieldIndices, i))) {
					continue;
				}
				Object fieldContent = row.getField(i);
				if (fieldContent instanceof LocalDateTime) {
					fieldContent = getLocalDateTimestamp((LocalDateTime) fieldContent);
				}
				data.add(fieldContent == null ? "" : fieldContent.toString());
			}
			return String.join(delimiterKey, data).getBytes();
		} catch (Throwable t) {
			throw new RuntimeException("Could not serialize row '" + row + "'. "
				+ "Make sure that the schema matches the input.", t);
		}
	}

	private boolean findEleExist(int[] array, int ele) {
		if (array.length == 0) {
			return false;
		}
		for (int e : array) {
			if (e == ele) {
				return true;
			}
		}
		return false;
	}
}
