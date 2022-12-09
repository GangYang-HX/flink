package com.bilibili.bsql.common.utils;

/**
 * @author: zhuzhengjun
 * @date: 2021/3/18 4:54 下午
 */
public class DateUtils {
	private DateUtils() {
	}

	/**
	 * 按毫秒计算的时间单位大小
	 */
	public final static int MILLISECONDS_ONE_SECOND               = 1000;
	public final static int MILLISECONDS_ONE_MINUTE               = 1000*60;
	public final static int MILLISECONDS_ONE_HOUR                 = 1000*60*60;
	public final static int MILLISECONDS_ONE_DAY                  = 1000*60*60*24;
	public final static int MILLISECONDS_ONE_WEEK                 = 1000*60*60*24*7;

	public static int convert2MinuteOfHour(long timestamp) {
		return (int) (timestamp % 3600000) / 60000;
	}

	public static Long getCurrentMinuteOfHour(long time) {
		return time % 3600000 / 60000;
	}
}
