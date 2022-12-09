package com.bilibili.bsql.common.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhangyang
 * @Date:2020/3/5
 * @Time:11:21 PM
 */
public final class DateReplaceUtil {
	private final static Pattern DATE_PATTERN = Pattern.compile("<%=([^>]*)%>");
	private final static String LOG_DATE = "log_date";
	private final static long ONE_DAY_MILLS = 24 * 60 * 60 * 1_000;

	private DateReplaceUtil() {
	}

	public static String replaceDateExpression(String content, int failoverDay) {
		if (StringUtils.isEmpty(content)) {
			return content;
		}
		Matcher matcher = DATE_PATTERN.matcher(content);

		while (matcher.find()) {
			String matcherItem = matcher.group();
			String expression = matcherItem.substring(3, matcherItem.length() - 2);
			if (StringUtils.equalsIgnoreCase(expression, LOG_DATE)) {
				DateFormat format = new SimpleDateFormat("yyyyMMdd");
				String date = format.format(new Date(System.currentTimeMillis() - ONE_DAY_MILLS - failoverDay * ONE_DAY_MILLS));
				return content.replaceAll(matcherItem, date);
			}
		}
		return content;
	}
}
