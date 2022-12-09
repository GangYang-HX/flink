package com.bilibili.bsql.hive.diversion;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/4 6:08 下午
 */
public class GetTableMeta extends ScalarFunction {


	public Map<String, Double> eval() throws Exception {
		Map<String, Double> map = new HashMap<>();
		map.put("ai.feature_merge_wdlks_multitask_pv", 0.9D);
		map.put("ai.feature_merge_wdlks_multitask_pv_1", 0.1D);
		return map;
	}


}
