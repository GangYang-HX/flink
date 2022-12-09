package com.bilibili.bsql.hive.diversion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhuzhengjun
 * @date: 2022/4/5 11:38 下午
 */
public class GetPartition extends ScalarFunction {
	public Tuple2<String, String[]> eval(String name) throws Exception {

		return Tuple2.of("ai.feature_merge_wdlks_multitask_pv", new String[]{});
	}
}
