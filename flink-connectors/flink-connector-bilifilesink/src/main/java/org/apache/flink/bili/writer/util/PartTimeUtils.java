package org.apache.flink.bili.writer.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: zhuzhengjun
 * @date: 2021/7/29 4:42 下午
 */
public class PartTimeUtils {

	public static final long HOUR_TIMESTAMP = 3600_000L;

	public static final long DAY_TIMESTAMP = 86400_000L;

	/**
	 * 获取当前小时的时间戳
	 *
	 * @param timestamp time
	 * @return 返回当前小时最小时间戳
	 */
	public static Long getHourTimestamp(long timestamp) {
		LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
		LocalDateTime nextHour = LocalDateTime.of(time.getYear(), time.getMonth(), time.getDayOfMonth(), time.getHour(), 0, 0);
		Instant instant = nextHour.atZone(ZoneId.systemDefault()).toInstant();
		return instant.toEpochMilli();
	}

	/**
	 * 获取当前时间天的时间戳
	 *
	 * @param timestamp timestamp
	 * @return 返回当前时间天级别最小时间戳
	 */
	public static Long getDayTimestamp(long timestamp) {
		LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
		LocalDateTime nextHour = LocalDateTime.of(time.getYear(), time.getMonth(), time.getDayOfMonth(), 0, 0, 0);
		Instant instant = nextHour.atZone(ZoneId.systemDefault()).toInstant();
		return instant.toEpochMilli();
	}

	/**
	 * 通过watermark获取上一个小时的时间
	 *
	 * @param timestamp time
	 * @return 返回上一个小时的时间
	 */
	public static Long getLastHourTimestamp(long timestamp) {
		return getHourTimestamp(timestamp) - HOUR_TIMESTAMP;
	}

	/**
	 * 通过watermakr获取前一天的时间
	 * @param timestamp timestamp
	 * @return 返回lastDay时间戳
	 */
	public static Long getLastDayTimestamp(long timestamp) {
		return getHourTimestamp(timestamp) - DAY_TIMESTAMP;
	}
	/**
	 *
	 * @param lastTimestamp 最后一次snapshot时间
	 * @param hourTimestamp 当前最小小时时间
	 * @return 返回中间相差的最小小时时间戳
	 */
	public static List<Long> getTimestampGap(long lastTimestamp, long hourTimestamp, long delay) {
		List<Long> result = new ArrayList<>();

		long tmpTimestamp = lastTimestamp;
		//避免分区提前创建
		while (tmpTimestamp <= hourTimestamp) {
			result.add(getHourTimestamp(tmpTimestamp));
			tmpTimestamp = tmpTimestamp + HOUR_TIMESTAMP;
		}
		//可以用creatIfNotExist规避
		result.add(getLastHourTimestamp(lastTimestamp));
		return result.isEmpty() ? null : result;
	}

	public static List<Long> getDayTimestampGap(long lastTimestamp, long dayTimestamp, long delay) {
		List<Long> result = new ArrayList<>();

		long tmpTimestamp = lastTimestamp;
		while (tmpTimestamp <= dayTimestamp) {
			result.add(tmpTimestamp);
			tmpTimestamp = tmpTimestamp + DAY_TIMESTAMP;
		}
		result.add(getLastDayTimestamp(lastTimestamp));

		return result.isEmpty() ? null : result;
	}


}
