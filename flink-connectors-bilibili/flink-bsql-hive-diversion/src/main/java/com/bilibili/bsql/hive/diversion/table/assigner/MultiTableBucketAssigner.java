package com.bilibili.bsql.hive.diversion.table.assigner;

import com.bilibili.bsql.hive.table.assigner.UdfBucketAssigner;
import com.bilibili.bsql.hive.utils.InvokeUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.bili.writer.ObjectIdentifier;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/30 10:50 下午
 */
public class MultiTableBucketAssigner extends UdfBucketAssigner {

	private static final Logger LOG = LoggerFactory.getLogger(MultiTableBucketAssigner.class);
	private final Map<String, String> tablePathMap;

	public MultiTableBucketAssigner(Class<?> multiPartitionUdfClass, int[] multiPartitionFieldIndex,
									String partitionUdf, String partitionUdfClass, String partitionBy,
									int eventTimePos, boolean allowDrift, boolean pathContainPartitionKey,
									Map<String, String> tablePathMap) {
		super(multiPartitionUdfClass, multiPartitionFieldIndex, partitionUdf, partitionUdfClass, partitionBy,
			eventTimePos, allowDrift, pathContainPartitionKey);
		this.tablePathMap = tablePathMap;
	}

	private String tableName;

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
			Tuple2<String, String[]> partitionTuple = (Tuple2<String, String[]>) multiPartitionMethod.invoke(multiPartitionInstance, args);
			partitionArray = partitionTuple.f1;
			tableName = partitionTuple.f0;
		} catch (IllegalAccessException | InvocationTargetException e) {
			LOG.error("invoke multi partition error , detail :{}", getErrorMessage(row, multiPartitionFieldIndex));
		}
		return partitionArray;
	}

	@Override
	public String getBucketId(Row element, Context context) {
		String bucketId = super.getBucketId(element, context);
		return this.tablePathMap.get(tableName) + SLASH_SYMBOL + bucketId;
	}
}
