/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kafka.table;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author zhouxiaogang
 * @version $Id: Util.java, v 0.1 2020-09-29 17:10
zhouxiaogang Exp $$
 */
public class ParseUtil {

	private static ObjectMapper objectMapper       = new ObjectMapper();

	public static boolean isJson(String str) {
		boolean flag = false;
		if (StringUtils.isNotEmpty(str)) {
			try {
				objectMapper.readValue(str, Map.class);
				flag = true;
			} catch (Throwable e) {
				flag = false;
			}
		}
		return flag;
	}

	public static boolean isTimeStamp(String str) {
		Date date = null;
		try {
			date = str.length() == 10 ? new Date(Long.parseLong(str) * 1000) : new Date(Long.parseLong(str));
		} catch (Exception e) {
			// ignore
		}
		return date != null;
	}

	public static Map<String,Object> ObjectToMap(Object obj) throws Exception{
		return objectMapper.readValue(objectMapper.writeValueAsBytes(obj), Map.class);
	}

	public static <T> T jsonStrToObject(String jsonStr, Class<T> clazz) throws
		JsonParseException, JsonMappingException, JsonGenerationException, IOException {
		return  objectMapper.readValue(jsonStr, clazz);
	}
}
