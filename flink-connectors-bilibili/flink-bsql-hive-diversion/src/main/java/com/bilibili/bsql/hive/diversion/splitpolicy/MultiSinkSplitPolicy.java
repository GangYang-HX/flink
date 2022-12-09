package com.bilibili.bsql.hive.diversion.splitpolicy;

import com.bilibili.bsql.hive.utils.InvokeUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.bili.writer.shuffle.policy.DefaultSplitPolicy;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: zhuzhengjun
 * @date: 2022/4/2 9:39 下午
 */
public class MultiSinkSplitPolicy<T extends Row> extends DefaultSplitPolicy<T> {


	private UserDefinedFunction tableTagInstance;
	private transient Method tableTagMethod;
	private final Class<?> tableTagUdfClass;
	private final int[] tableTagFieldIndex;

	private final Map<String, Double> customSplitPolicyMap;

	private void init() {
		if (tableTagUdfClass != null) {
			tableTagInstance = InvokeUtils.initInstance(tableTagUdfClass);
		}
	}

	public MultiSinkSplitPolicy(Map<String, String> properties, Map<String, Double> tableMetaMap,
								Class<?> tableTagUdfClass, int[] tableTagFieldIndex,
								Map<String, Double> customSplitPolicyMap) {

		super(properties, tableMetaMap);
		this.tableTagUdfClass = tableTagUdfClass;
		this.tableTagFieldIndex = tableTagFieldIndex;
		this.customSplitPolicyMap = customSplitPolicyMap;
		init();
	}


	@Override
	public String getTableTag(Row row) {
		String tableTag = "";
		if (tableTagUdfClass == null) {
			return tableTag;
		}
		if (tableTagMethod == null) {
			tableTagMethod = InvokeUtils.initMethod(tableTagUdfClass);
		}

		try {
			Object[] args = new Object[tableTagFieldIndex.length];
			for (int i = 0; i < args.length; i++) {
				args[i] = row.getField(tableTagFieldIndex[i]);
			}
			tableTag = (String) tableTagMethod.invoke(tableTagInstance, args);
		} catch (IllegalAccessException | InvocationTargetException e) {
			//ignore
		}
		return tableTag;
	}

	@Override
	public Map<String, Tuple2<Integer, Integer>> getTableTagMap(int sinkParallelism) {
		return super.getTableTagMap(sinkParallelism);
	}

	@Override
	protected Map<String, Integer> getDirectMap(int maxParallelism) {
		return CollectionUtil.isNullOrEmpty(customSplitPolicyMap) ? super.getDirectMap(maxParallelism) :
			customSplitPolicyMap.entrySet()
				.stream()
				.collect(Collectors.toMap(Map.Entry::getKey, meta -> (int) (meta.getValue() * maxParallelism)));


	}
}
