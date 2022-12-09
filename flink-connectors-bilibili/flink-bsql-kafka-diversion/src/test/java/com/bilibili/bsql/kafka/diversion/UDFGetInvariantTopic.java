package com.bilibili.bsql.kafka.diversion;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class UDFGetInvariantTopic extends ScalarFunction {

	public String eval(String topicName) {
		if (StringUtils.isBlank(topicName))
			return null;

		return topicName;
	}

}
