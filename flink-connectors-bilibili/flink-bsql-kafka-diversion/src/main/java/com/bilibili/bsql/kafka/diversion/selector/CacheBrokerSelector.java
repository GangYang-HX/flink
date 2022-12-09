package com.bilibili.bsql.kafka.diversion.selector;

import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;

import com.bilibili.bsql.kafka.diversion.cache.GuavaCacheFactory;
import com.bilibili.bsql.kafka.diversion.cache.KeyUtil;
import com.google.common.cache.Cache;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.Arrays;

/** CacheBrokerSelector. */
public class CacheBrokerSelector implements TopicSelector<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CacheTopicSelector.class);

    private final int[] brokerFiledIndex;
    private final Class<?> brokerUdfClass;
    private UserDefinedFunction brokerUdfInstance;
    private transient Method brokerEvalMethod;
    private final DynamicTableSink.DataStructureConverter converter;
    private Cache<String, String> brokerUdfCache;

    public CacheBrokerSelector(
            int cacheTtl,
            @Nullable Class<?> brokerUdfClass,
            DynamicTableSink.DataStructureConverter converter,
            int[] brokerFiledIndex) {
        this.brokerFiledIndex = brokerFiledIndex;
        this.brokerUdfClass = brokerUdfClass;
        this.converter = converter;
        if (brokerUdfClass != null) {
            brokerUdfInstance = initInstance(brokerUdfClass);
            brokerUdfCache = new GuavaCacheFactory<String, String>().getCache(cacheTtl);
        }
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

    @Override
    public String apply(RowData rowData) {
        if (brokerUdfClass == null) {
            return null;
        }
        if (brokerEvalMethod == null) {
            brokerEvalMethod = initMethod(brokerUdfClass);
        }
        String brokers = null;
        Row row = (Row) converter.toExternal(rowData);
        try {
            Object[] args = new Object[brokerFiledIndex.length];
            for (int i = 0; i < args.length; i++) {
                args[i] = row.getField(brokerFiledIndex[i]);
            }
            String key = KeyUtil.transformKey(args);
            if (KeyUtil.UNCACHE_KEY.equals(key)) {
                // don't cache
                LOG.warn("key: {} don't cache", key);
                brokers = (String) brokerEvalMethod.invoke(brokerUdfInstance, args);
            } else {
                brokers =
                        brokerUdfCache.get(
                                key,
                                () -> (String) brokerEvalMethod.invoke(brokerUdfInstance, args));
            }
        } catch (Exception e) {
            LOG.error(
                    "bootstrapServers udf call error, detail: {}",
                    getErrorMessage(row, brokerFiledIndex),
                    e);
        }
        if (StringUtils.isBlank(brokers)) {
            throw new RuntimeException("UDF获取 brokers 地址异常!");
        }
        return brokers;
    }

    private String getErrorMessage(Row row, int[] fieldIndex) {
        StringBuilder msg = new StringBuilder();
        if (row != null) {
            Arrays.stream(fieldIndex)
                    .forEach(
                            i -> {
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
}
