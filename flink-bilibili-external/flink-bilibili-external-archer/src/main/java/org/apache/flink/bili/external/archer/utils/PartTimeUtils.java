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

package org.apache.flink.bili.external.archer.utils;

import org.apache.flink.bili.external.archer.constant.ArcherConstants;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utilities of part time. */
public class PartTimeUtils {
    private static final Pattern PARTITION_DAY_PATTERN =
            Pattern.compile("(\\d{4})(\\d{1,2})(\\d{1,2})");
    private static final Pattern PARTITION_HOUR_PATTERN = Pattern.compile("(\\d{1,2})");

    public static LocalDateTime getDayLocalDateTime(String date) throws Exception {
        Matcher m = PARTITION_DAY_PATTERN.matcher(date);
        if (m.find()) {
            return parserLocalDateTime(m.group() + "/00");
        } else {
            throw new Exception("PartTimeUtils getDayLocalDateTime error.");
        }
    }

    public static LocalDateTime getHourLocalDateTime(String date, String hour) throws Exception {
        Matcher dayMatch = PARTITION_DAY_PATTERN.matcher(date);
        Matcher hourMatch = PARTITION_HOUR_PATTERN.matcher(hour);
        String day, hours;
        if (dayMatch.find()) {
            day = dayMatch.group();
        } else {
            throw new Exception("PartTimeUtils getHourLocalDateTime day string error.");
        }
        if (hourMatch.find()) {
            hours = hourMatch.group();
        } else {
            throw new Exception("PartTimeUtils getHourLocalDateTime hour string error.");
        }
        return parserLocalDateTime(day + "/" + hours);
    }

    private static LocalDateTime parserLocalDateTime(String timeStr) {
        return LocalDateTime.from(ArcherConstants.HOUR_FORMAT.parse(timeStr));
    }

    public static final String LOG_DATE = "log_date";
    public static final String LOG_HOUR = "log_hour";
    public static final List<String> COMMON_KEYS = Arrays.asList(LOG_DATE, LOG_HOUR);

    public static int containsKey(List<String> partitionKeys) {
        int containKey = 0;
        for (int i = partitionKeys.size() - 1; i >= 0; i--) {
            if (COMMON_KEYS.contains(partitionKeys.get(i))) {
                containKey++;
            }
        }
        return containKey;
    }
}
