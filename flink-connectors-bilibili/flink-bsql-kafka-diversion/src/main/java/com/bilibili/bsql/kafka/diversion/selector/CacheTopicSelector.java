
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

/** CacheTopicSelector. */
public class CacheTopicSelector implements TopicSelector<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CacheTopicSelector.class);

    private Cache<String, String> topicUdfCache;

    private final Class<?> topicUdfClass;

    private UserDefinedFunction topicUdfInstance;

    private transient Method topicEvalMethod;

    private final DynamicTableSink.DataStructureConverter converter;

    private final int[] topicFiledIndex;

    public CacheTopicSelector(
            int cacheTtl,
            @Nullable Class<?> topicUdfClass,
            DynamicTableSink.DataStructureConverter converter,
            int[] topicFiledIndex) {
        this.topicUdfClass = topicUdfClass;
        this.converter = converter;
        this.topicFiledIndex = topicFiledIndex;
        if (topicUdfClass != null) {
            topicUdfInstance = initInstance(topicUdfClass);
            topicUdfCache = new GuavaCacheFactory<String, String>().getCache(cacheTtl);
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

    private UserDefinedFunction initInstance(Class<?> udfClass) {
        try {
            UserDefinedFunction udfInstance = (UserDefinedFunction) udfClass.newInstance();
            udfInstance.open(null);
            return udfInstance;
        } catch (Exception e) {
            throw new RuntimeException("create instance error.");
        }
    }

    @Override
    public String apply(RowData element) {
        if (topicUdfClass == null) {
            return null;
        }
        if (topicEvalMethod == null) {
            topicEvalMethod = initMethod(topicUdfClass);
        }
        String targetTopic = null;
        Row row = (Row) converter.toExternal(element);
        try {
            Object[] args = new Object[topicFiledIndex.length];
            for (int i = 0; i < args.length; i++) {
                args[i] = row.getField(topicFiledIndex[i]);
            }
            String key = KeyUtil.transformKey(args);
            if (KeyUtil.UNCACHE_KEY.equals(key)) {
                // no cache
                LOG.warn("key: {} don't cache", key);
                targetTopic = (String) topicEvalMethod.invoke(topicUdfInstance, args);
            } else {
                targetTopic =
                        topicUdfCache.get(
                                key, () -> (String) topicEvalMethod.invoke(topicUdfInstance, args));
            }
        } catch (Exception e) {
            LOG.error("topic udf call error, detail: {}", getErrorMessage(row, topicFiledIndex), e);
        }
        if (StringUtils.isBlank(targetTopic)) {
            throw new RuntimeException("UDF获取 Topic 异常!");
        }
        return targetTopic;
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
