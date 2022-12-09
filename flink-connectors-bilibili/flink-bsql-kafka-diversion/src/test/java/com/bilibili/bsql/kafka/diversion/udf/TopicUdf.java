package com.bilibili.bsql.kafka.diversion.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class TopicUdf extends ScalarFunction {

	public String eval(String brokerArg, String topicArg) throws Exception {
		System.out.println("UDF call: ->" + brokerArg + "|" + topicArg);
		switch (brokerArg) {
			case "cluster1":
				switch (topicArg) {
					case "topic1":
						return "cluster1_1";
					case "topic2":
						return "cluster1_2";
					default:
						throw new Exception("error");
				}
			case "cluster2":
				switch (topicArg) {
					case "topic1":
						return "cluster2_1";
					case "topic2":
						return "cluster2_2";
					default:
						throw new Exception("error");
				}
			default:
				throw new Exception("error");
		}
	}

}
