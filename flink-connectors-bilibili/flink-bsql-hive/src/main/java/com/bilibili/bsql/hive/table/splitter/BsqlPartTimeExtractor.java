package com.bilibili.bsql.hive.table.splitter;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.bili.writer.extractor.PartitionTimeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class BsqlPartTimeExtractor implements PartitionTimeExtractor {

	private static final Logger LOG = LoggerFactory.getLogger(BsqlPartTimeExtractor.class);
	private final static TimeZone ZONE = TimeZone.getTimeZone("GMT+8");
	private static final ThreadLocal<SimpleDateFormat> FORMATTER_CACHE = new ThreadLocal<>();
	public static final String DAY_PATTERN = "yyyyMMdd";
	public static final String HOUR_PATTERN = "yyyyMMddHH";
	public static final String MINUTE_PATTERN = "yyyyMMddHHmm";
	public static final String LOG_DATE = "log_date";
	public static final String LOG_HOUR = "log_hour";
	public static final String LOG_MINUTE = "log_minute";
	public static final List<String> COMMON_KEYS = Arrays.asList(LOG_DATE, LOG_HOUR, LOG_MINUTE);

	/**
	 * berserker平台约束：
	 * 1：log_date
	 * 2: log_date/log_hour
	 * 3: log_date/log_hour/log_minute
	 *
	 * @param partitionKeys   partitionKeys
	 * @param partitionValues partitionValues
	 * @return part time
	 */
	@Override
	public Long extract(List<String> partitionKeys, List<String> partitionValues) {
		long timestamp = System.currentTimeMillis();
		LOG.info(String.format("PartitionsKeys = %s, PartitionsValue = %s", partitionKeys, partitionValues));

		try {
			if (CollectionUtils.isNotEmpty(partitionKeys) && CollectionUtils.isNotEmpty(partitionValues)) {
				int containKey = containsKey(partitionKeys);
				if (containKey == 1) {
					return getPartTime(DAY_PATTERN, partitionValues.get(0),
						Calendar.DAY_OF_MONTH);
				} else if (containKey == 2) {
					return getPartTime(HOUR_PATTERN, partitionValues.get(0) + partitionValues.get(1),
						Calendar.HOUR_OF_DAY);
				} else if (containKey == 3) {
					return getPartTime(MINUTE_PATTERN,
						partitionValues.get(0) + partitionValues.get(1) + partitionValues.get(2),
						Calendar.MINUTE);
				}
			}
		} catch (
			Exception e) {
			LOG.error("extract timestamp error,", e);
		}
		LOG.warn(String.format("use default timestamp = %s", timestamp));
		return timestamp;
	}

	private Long getPartTime(String pattern, String timeString, Integer calender) throws ParseException {
		SimpleDateFormat dateTimeFormat = FORMATTER_CACHE.get();
		Calendar calendar = Calendar.getInstance();
		if (null == dateTimeFormat) {
			dateTimeFormat = new SimpleDateFormat(pattern);
			dateTimeFormat.setTimeZone(ZONE);
			FORMATTER_CACHE.set(dateTimeFormat);
		}
		calendar.setTime(dateTimeFormat.parse(timeString));
		calendar.add(calender, 1);
		LOG.info("the partition time is : " + calendar.getTime());
		return calendar.getTime().getTime();
	}

	private int containsKey(List<String> partitionKeys) {
		int containKey = 0;
		for (int i = partitionKeys.size() - 1; i >= 0; i--) {
			if (COMMON_KEYS.contains(partitionKeys.get(i))) {
				containKey++;
			}
		}
		return containKey;
	}
}
