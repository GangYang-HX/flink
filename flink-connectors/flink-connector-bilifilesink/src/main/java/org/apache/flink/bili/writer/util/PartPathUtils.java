package org.apache.flink.bili.writer.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @author: zhuzhengjun
 * @date: 2021/7/30 2:45 下午
 */
public class PartPathUtils {
	private final static ZoneId RUNTIME_TIMEZONE = ZoneId.systemDefault();
	public static final String DAY_PATTERN = "yyyyMMdd";
	public static final String HOUR_PATTERN = "yyyyMMdd/HH";
	public static final String LOG_HOUR = "log_hour=";
	public static final String LOG_HOUR_KEY = "log_hour";
	public static final String LOG_DATE = "log_date=";
	public static final String LOG_DATE_KEY = "log_date";
	public static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern(HOUR_PATTERN);
	public static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern(DAY_PATTERN);


	public static String concatPartitionPath(boolean pathContainPartitionKey, long recordingTime, List<String> partitionKeys) {
		return concatPartitionPathWithTimezone(pathContainPartitionKey, recordingTime, null, partitionKeys);
	}

	public static String concatPartitionPathWithTimezone(boolean pathContainPartitionKey, long recordingTime, String timezone, List<String> partitionKeys) {
		String result;
		String logDatePrefix = pathContainPartitionKey ? LOG_DATE : "";
		String logHourPrefix = pathContainPartitionKey ? LOG_HOUR : "";

		ZoneId zone = getTimezone(timezone);
		if (PartPathUtils.isHourPath(partitionKeys)) {
			String formatTime = HOUR_FORMAT.format(getDateTimeOfTimestamp(recordingTime, zone));
			result = logDatePrefix + formatTime.substring(0, 8) + "/" + logHourPrefix + formatTime.substring(9, 11);
		} else {
			String formatTime = DAY_FORMAT.format(getDateTimeOfTimestamp(recordingTime, zone));
			result =  logDatePrefix + formatTime;
		}
		return result;
	}

	public static LocalDateTime getDateTimeOfTimestamp(long timestamp, ZoneId zoneId) {
		return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);
	}

	private static ZoneId getTimezone(String timezone) {
		if (timezone == null) {
			return RUNTIME_TIMEZONE;
		}

		ZoneId zone = ZoneId.of(timezone);
		// timezone invalid
		if (zone.getId().equalsIgnoreCase("GMT")) {
			return RUNTIME_TIMEZONE;
		}
		return zone;
	}

	/**
	 * 判断是否为天级别
	 * @param path 路径
	 * @return 返回天级别
	 */
	public static boolean isDailyPath(String path) {
		return path.contains(LOG_DATE) && !path.contains(LOG_HOUR);
	}
	/**
	 * 判断是否为小时分区
	 * @param partitionKeys 分区字段
	 * @return 返回小时分区
	 */
	public static boolean isHourPath(List<String> partitionKeys) {
		return partitionKeys.contains(LOG_HOUR_KEY);
	}

	/**
	 * 初始化part
	 * @param partitionKeys  分区主键
	 * @return 返回分区初始化的partition
	 *
	 */
	public static String genPartition(List<String> partitionKeys) {
		return partitionKeys.contains(LOG_HOUR_KEY) ? LOG_DATE + "19700101" + "/" + LOG_HOUR + "01" : LOG_DATE + "19700101";
	}
}
