package com.bilibili.bsql.kafka.util;

import java.util.Date;

/** ParseUtils. */
public class ParseUtils {

    public static boolean isTimeStamp(String str) {
        Date date = null;
        try {
            date =
                    str.length() == 10
                            ? new Date(Long.parseLong(str) * 1000)
                            : new Date(Long.parseLong(str));
        } catch (Exception e) {
            // ignore
        }
        return date != null;
    }
}
