package com.bilibili.bsql.hive.table.assigner;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.types.Row;


/**
 * @author: zhuzhengjun
 * @date: 2021/12/29 8:08 下午
 */
public interface BucketIdGenerator {
	String LOG_DATE = "log_date";
	String LOG_HOUR = "log_hour";
	String SLASH_SYMBOL = "/";
	String EQUAL_SYMBOL = "=";



	/**
	 * in order to
	 *
	 * @return k -> partition key , v -> partition value
	 */
	String[] genBucketIdArray(Row row, BucketAssigner.Context context);


	/**
	 * @param partitionBy partition key string
	 * @return partition kes array
	 */
	default String[] partitionKeys(String partitionBy) {
		return StringUtils.split(partitionBy, ";");
	}


	default boolean useProcTime(Long watermark, int eventTimePos) {
		return watermark == Long.MIN_VALUE || eventTimePos == -1;
	}

	default Long getBucketTime(Boolean useProcTime, Row element, int eventTimePos, Long watermark) {
		if (useProcTime) {
			// without watermark or without eventTimePos, bucket time is proc time
			return System.currentTimeMillis();
		} else {
			// event time
			return (Long) element.getField(eventTimePos);
		}
	}

	String genBucketId(String buckedId, String[] partitionKeys,String[] buckedIdMap, boolean pathContainPartitionKey);


}
