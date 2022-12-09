package com.bilibili.bsql.hive.diversion;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhuzhengjun
 * @date: 2022/4/2 10:14 下午
 */
public class GetTableTag extends ScalarFunction {
	public String eval(String name, Integer num) throws Exception {
		if (name.hashCode() % 2 == 0) {
			return "ai.feature_merge_wdlks_multitask_pv";
		}else{
			return "ai.feature_merge_wdlks_multitask_pv_1";
		}
	}

}
