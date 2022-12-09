package com.bilibili.bsql.hive.table.assigner;


import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by haozhugogo on 2020/4/29.
 */
public class BiliBucketAssigner implements BucketAssigner<Row, String>, BucketIdGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(BiliBucketAssigner.class);


	protected final String partitionBy;
	protected final int eventTimePos;
	protected boolean allowDrift;
	protected boolean pathContainPartitionKey;

	protected final static TimeZone ZONE = TimeZone.getTimeZone("GMT+8");
	protected static final ThreadLocal<SimpleDateFormat> FORMATTER_CACHE = new ThreadLocal<>();
	protected String partitionUdf;
	protected String partitionUdfClass;
	protected Class<?> multiPartitionUdfClass;
	protected int[] multiPartitionFieldIndex;
	protected String[] partitionKeysArray;


	@Override
	public String getBucketId(Row element, Context context) {
		String[] partitions = this.genBucketIdArray(element, context);
		String bucketId = "";
		bucketId = this.genBucketId(bucketId, partitionKeys(this.partitionBy), partitions, pathContainPartitionKey);
		return bucketId;
	}

	@Override
	public SimpleVersionedSerializer<String> getSerializer() {
		return SimpleVersionedStringSerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return "BiliBucketAssigner";
	}

	public BiliBucketAssigner(final Class<?> multiPartitionUdfClass, final int[] multiPartitionFieldIndex,
							  final String partitionUdf, final String partitionUdfClass, String partitionBy,
							  int eventTimePos, boolean allowDrift, boolean pathContainPartitionKey) {

		this.partitionBy = partitionBy;
		this.eventTimePos = eventTimePos;
		this.allowDrift = allowDrift;
		this.pathContainPartitionKey = pathContainPartitionKey;
		this.partitionUdf = partitionUdf;
		this.partitionUdfClass = partitionUdfClass;
		this.multiPartitionFieldIndex = multiPartitionFieldIndex;
		this.multiPartitionUdfClass = multiPartitionUdfClass;
		this.partitionKeysArray = partitionKeys(this.partitionBy);
	}

	public BiliBucketAssigner(String partitionBy, int eventTimePos, boolean allowDrift, boolean pathContainPartitionKey) {
		this.partitionBy = partitionBy;
		this.eventTimePos = eventTimePos;
		this.allowDrift = allowDrift;
		this.pathContainPartitionKey = pathContainPartitionKey;
	}

	@Override
	public String[] genBucketIdArray(Row element, Context context) {
		Long wm = context.currentWatermark();
		boolean useProcTime = useProcTime(wm, eventTimePos);
		long bucketTime = getBucketTime(useProcTime, element, eventTimePos, wm);
		Date bucketDate = new Date(bucketTime);
		SimpleDateFormat dateTimeFormat = FORMATTER_CACHE.get();
		if (null == dateTimeFormat) {
			dateTimeFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
			dateTimeFormat.setTimeZone(ZONE);
			FORMATTER_CACHE.set(dateTimeFormat);
		}
		String formatTime = dateTimeFormat.format(bucketDate);
		String[] partitionKeys = partitionKeys(this.partitionBy);
		String[] bucketIdArray = new String[partitionKeys.length];
		for (int i = 0; i < partitionKeys.length; i++) {
			switch (partitionKeys[i]) {
				case LOG_DATE:
					bucketIdArray[i] = formatTime.substring(0, 8);
					break;
				case LOG_HOUR:
					bucketIdArray[i] = formatTime.substring(9, 11);
					break;
			}

		}
		return bucketIdArray;
	}

	@Override
	public Long getBucketTime(Boolean useProcTime, Row element, int eventTimePos, Long wm) {
		if (useProcTime) {
			// without watermark or without eventTimePos, bucket time is proc time
			return System.currentTimeMillis();
		} else {

			// event time
			Long bucketTime =  (Long) element.getField(eventTimePos);
			// allow partition drift when eventTime < wm
			if (allowDrift && bucketTime < wm) {
				bucketTime = wm;
			}
			return bucketTime;
		}

	}

	@Override
	public String genBucketId(String bucketId, String[] partitionKeys, String[] bucketIdArray, boolean pathContainPartitionKey) {
		Preconditions.checkArgument(partitionKeys != null && partitionKeys.length != 0, "partitionKeys must not be null");
		Preconditions.checkArgument(bucketIdArray != null && bucketIdArray.length != 0, "buckedIdArray must not be null");
		StringBuilder bucketIdBuilder = new StringBuilder(bucketId);
		for (int i = 0; i < partitionKeys.length; i++) {
			if (bucketIdArray[i] != null) {
				if (pathContainPartitionKey) {
					bucketIdBuilder.append(SLASH_SYMBOL).append(partitionKeys[i]).append(EQUAL_SYMBOL).append(bucketIdArray[i]);
				} else {
					bucketIdBuilder.append(SLASH_SYMBOL).append(bucketIdArray[i]);
				}
			}
		}
		bucketId = bucketIdBuilder.toString();
		return bucketId;
	}

}

