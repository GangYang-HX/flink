/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.metrics.CharacterFilter;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class MapUtil {
	private static final Pattern ANGLE_BRACKETS_CHAR_PATTERN = Pattern.compile("<\\S+>");

	private static final int MAX_CHARACTER_LENGTH = 500;
	private static final int MAX_CHARACTER_LENGTH_HALF = MAX_CHARACTER_LENGTH >> 1;
	private static final CharacterFilter CHARACTER_FILTER = input ->
		MAX_CHARACTER_LENGTH > input.length() ?
			input :
			StringUtils.substring(input, 0, MAX_CHARACTER_LENGTH_HALF) +
				StringUtils.substring(input, input.length() - MAX_CHARACTER_LENGTH_HALF, input.length());

	public static Map<String, String> filterMap(Map<String, String> map) {
		// if keys are surrounded by brackets: remove them, transforming "<name>" to "name".
		Map<String, String> filterMap = new HashMap<>();

		for (Map.Entry<String, String> entry : map.entrySet()) {
			String key = entry.getKey();
			if (ANGLE_BRACKETS_CHAR_PATTERN.matcher(key).find()) {
				key = key.substring(1, key.length() - 1);
			}
			String value = entry.getValue() == null ? "" : CHARACTER_FILTER.filterCharacters(entry.getValue());
			filterMap.put(key, value);
		}

		return filterMap;
	}

	public static String mapToEscapedString(Map<String, String> map) {
		StringBuilder builder = new StringBuilder();
		Set<String> keySet = map.keySet();
		for (String key : keySet) {
			builder.append(key);
			builder.append("=\"");
			builder.append(escapedString(map.get(key)));
			builder.append("\",");
		}
		if (builder.length() > 1) {
			builder.deleteCharAt(builder.length() - 1);
		}
		return builder.toString();
	}

	public static String escapedString(String s) {
		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
				case '\\':
					builder.append("\\\\");
					break;
				case '\"':
					builder.append("\\\"");
					break;
				case '\n':
					builder.append("\\n");
					break;
				default:
					builder.append(c);
			}
		}

		return builder.toString();
	}


	/**
	 * parse kv string with format like: k1=v1;k2=v2
	 *
	 * @param kvString kv string concatenated with semicolon
	 * @return
	 */
	public static Map<String, String> parseKvString(final String kvString) {
		Map<String, String> map = new HashMap<>();

		if (StringUtils.isNotBlank(kvString)) {
			String[] kvs = kvString.split(";");
			for (String kv : kvs) {
				int idx = kv.indexOf("=");
				if (idx > 0) {
					String key = kv.substring(0, idx);
					String value = kv.substring(idx + 1);
					if (StringUtils.isNotBlank(key)) {
						map.put(key, value);
					}
				}
			}
		}

		return map;
	}

}
