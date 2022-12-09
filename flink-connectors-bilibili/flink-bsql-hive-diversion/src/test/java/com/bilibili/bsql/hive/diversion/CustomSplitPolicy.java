package com.bilibili.bsql.hive.diversion;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: zhuzhengjun
 * @date: 2022/4/26 10:51 上午
 */
public class CustomSplitPolicy extends ScalarFunction {
	public static Map<String, Double> eval()  {
		Map<String, Double> map = new HashMap<>();
		map.put("event_type=click", 0.017D);
		map.put("event_type=pv", 0.017D);
		map.put("event_type=show/app_id=1", 0.15D);
		map.put("event_type=show", 0.017D);
		map.put("event_type=sys/app_id=1", 0.75D);
		map.put("event_type=sys", 0.017D);
		map.put("event_type=sys/app_id=5", 0.0334D);
		return map;
	}



}
