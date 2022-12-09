package com.bilibili.bsql.hive.table.assigner;


import com.bilibili.bsql.hive.utils.InvokeUtils;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;


/**
 * @author: zhuzhengjun
 * @date: 2021/12/29 8:08 下午
 */
public class UdfBucketAssigner extends BiliBucketAssigner implements BucketIdGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(UdfBucketAssigner.class);

	protected UserDefinedFunction multiPartitionInstance;
	protected transient Method multiPartitionMethod;


	public UdfBucketAssigner(
		Class<?> multiPartitionUdfClass, int[] multiPartitionFieldIndex, String partitionUdf, String partitionUdfClass,
		String partitionBy, int eventTimePos, boolean allowDrift, boolean pathContainPartitionKey) {
		super(multiPartitionUdfClass, multiPartitionFieldIndex, partitionUdf, partitionUdfClass, partitionBy,
			eventTimePos, allowDrift, pathContainPartitionKey);
		init();
	}

	protected void init() {
		if (multiPartitionUdfClass != null) {
			multiPartitionInstance = InvokeUtils.initInstance(multiPartitionUdfClass);
		}
	}

	@Override
	public String[] genBucketIdArray(Row row, Context context) {
		String[] partitionArray = new String[0];
		if (multiPartitionUdfClass == null) {
			return partitionArray;
		}
		if (multiPartitionMethod == null) {
			multiPartitionMethod = InvokeUtils.initMethod(multiPartitionUdfClass);
		}

		try {
			Object[] args = new Object[multiPartitionFieldIndex.length];
			for (int i = 0; i < args.length; i++) {
				args[i] = row.getField(multiPartitionFieldIndex[i]);
			}
			partitionArray = (String[]) multiPartitionMethod.invoke(multiPartitionInstance, args);
		} catch (IllegalAccessException | InvocationTargetException e) {
			LOG.error("invoke multi partition error , detail :{}", getErrorMessage(row, multiPartitionFieldIndex));
		}
		return partitionArray;
	}


	protected String getErrorMessage(Row row, int[] fieldIndex) {
		StringBuilder msg = new StringBuilder();
		if (row != null) {
			Arrays.stream(fieldIndex).forEach(i -> {
				Object fieldValue = row.getField(i);
				if (fieldValue != null) {
					msg.append(fieldValue).append(",");
				} else {
					msg.append("null").append(",");
				}
			});
		}
		return msg.toString();
	}


	@Override
	public String getBucketId(Row element, Context context) {
		StringBuilder bucketId = new StringBuilder(super.genBucketId("", partitionKeysArray,
			super.genBucketIdArray(element, context), pathContainPartitionKey));
		String[] partitions = genBucketIdArray(element, context);
		for (String partition : partitions) {
			bucketId.append(SLASH_SYMBOL).append(partition);
		}
		return bucketId.toString();
	}


}
