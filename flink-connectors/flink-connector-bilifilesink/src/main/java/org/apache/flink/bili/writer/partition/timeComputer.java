/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bili.writer.partition;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.TimeZone;

import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.types.Row;

/**
 *
 * @author zhouxiaogang
 * @version $Id: timeComputer.java, v 0.1 2020-05-21 18:00
zhouxiaogang Exp $$
 */
public class timeComputer implements PartitionComputer<Row> {
	private String                                     partitionBy;
	private final static TimeZone zone            = TimeZone.getTimeZone("GMT+8");
	private static final ThreadLocal<SimpleDateFormat> FORMATTER_CACHE = new ThreadLocal();
	private long                                       initWatermark   = Long.MIN_VALUE;

	public LinkedHashMap<String, String> generatePartValues(Row in, BucketAssigner.Context context)
		throws Exception {
		Long wm = context.currentWatermark();
		if (initWatermark == wm) {
			wm = System.currentTimeMillis();
		}
		Date date = new Date(wm);
		SimpleDateFormat dateTimeFormat = FORMATTER_CACHE.get();
		if (null == dateTimeFormat) {
			dateTimeFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
			dateTimeFormat.setTimeZone(zone);
			FORMATTER_CACHE.set(dateTimeFormat);
		}
		String formatTime = dateTimeFormat.format(date);
		LinkedHashMap bucketId = new LinkedHashMap<String, String>();
		if ("log_date".equals(partitionBy)) {
//			bucketId = "log_date=" + formatTime.substring(0, 8);
			bucketId.put("log_date", formatTime.substring(0, 8));
		} else {
//			bucketId = "log_date=" + formatTime.substring(0, 8) + "/" + "log_hour=" + formatTime.substring(9, 11);
			bucketId.put("log_date", formatTime.substring(0, 8));
			bucketId.put("log_hour", formatTime.substring(9, 11));
		}
		return bucketId;
	}

	public timeComputer(String partitionBy) {
		this.partitionBy = partitionBy;
	}
}
