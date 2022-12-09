package com.bilibili.bsql.kafka.diversion;

import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.StringUtils;

/** UDFGetInvariantTopic. */
public class UDFGetInvariantTopic extends ScalarFunction {

    public String eval(String topicName) {
        if (StringUtils.isBlank(topicName)) {
            return null;
        }

        return topicName;
    }
}
