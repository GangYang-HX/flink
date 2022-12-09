package com.bilibili.bsql.mysql.sharding;

import com.bilibili.bsql.common.global.SymbolsConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingValue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className AdminIdShardingAlgorithm.java
 * @description This is the description of AdminIdShardingAlgorithm.java
 * @createTime 2020-10-22 15:18:00
 */
@Slf4j
public class AdminIdShardingAlgorithm implements ComplexKeysShardingAlgorithm {
	private final Tuple3<Method, String, Class> shardingMethodAndColumns;

	private final String[] bucket;

	public AdminIdShardingAlgorithm(Tuple3<Method, String, Class> shardingMethodAndColumns, String[] bucket) {
		this.shardingMethodAndColumns = shardingMethodAndColumns;
		this.bucket = bucket;
	}

	@Override
	public Collection<String> doSharding(Collection availableTargetNames, ComplexKeysShardingValue shardingValue) {
		Integer bucketSize = Integer.parseInt(shardingMethodAndColumns.f1.substring(0, shardingMethodAndColumns.f1.indexOf(SymbolsConstant.COMMA)));
		String[] inputColumnNames = shardingMethodAndColumns.f1.substring(shardingMethodAndColumns.f1.indexOf(SymbolsConstant.COMMA) + 1).split(SymbolsConstant.COMMA);
		Object[] inputColumnValues = new Object[inputColumnNames.length];
		for (int i = 0; i < inputColumnNames.length; i++) {
			Collection valueList = (Collection) (shardingValue.getColumnNameAndShardingValuesMap().get(inputColumnNames[i]));
			inputColumnValues[i] = valueList.toArray()[0];
		}

		Object[] argumentSet;
		if (shardingMethodAndColumns.f0.getParameterTypes()[1] == Object[].class) {
			argumentSet = new Object[]{bucketSize, inputColumnValues};
		} else {
			ArrayList<Object> argumentList = new ArrayList<>();
			argumentList.add(bucketSize);
			argumentList.addAll(Arrays.asList(inputColumnValues));
			argumentSet = argumentList.toArray();
		}

		List<Integer> indexList;
		List<String> ret = new ArrayList<>();
		try {
			indexList = (List<Integer>) shardingMethodAndColumns.f0.invoke(shardingMethodAndColumns.f2.newInstance(), argumentSet);
			for (Integer index : indexList) {
				ret.add(bucket[index]);
			}
		} catch (Exception e) {
			throw new RuntimeException(e.getCause());
		}
		return ret;
	}
}
