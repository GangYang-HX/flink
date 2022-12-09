package com.bilibili.bsql.hive;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;

public class PartitionUdf extends ScalarFunction {

	public String[] eval(String test) throws Exception {
		if (test.hashCode() % 2 == 0) {
			return Arrays.asList("appid=2", "event=3").toArray(new String[0]);
		}else {
			return Arrays.asList("appid=10", "event=10").toArray(new String[0]);
		}
	}

}
