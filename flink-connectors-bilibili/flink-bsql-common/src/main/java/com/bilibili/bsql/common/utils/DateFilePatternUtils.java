package com.bilibili.bsql.common.utils;

import com.bilibili.bsql.common.global.SymbolsConstant;
import com.bilibili.bsql.common.global.UnitConstant;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author zhuzhengjun
 * @date 2020/10/28 5:06 下午
 */
public class DateFilePatternUtils {
    private final static String yyyyMMdd   = "yyyyMMdd";
    private final static String yyyyMMddHH = "yyyyMMddHH";

    public static LinkedHashMap<String, String> datePatternConvert(String pathPattern) {
        int start = pathPattern.lastIndexOf(SymbolsConstant.LEFT_BRACKET);
        int end = pathPattern.lastIndexOf(SymbolsConstant.RIGHT_BRACKET);

        LinkedHashMap<String, String> ret = new LinkedHashMap<>();
        if (start == -1 || end == -1) {
            ret.put("global", pathPattern);
            return ret;
        }
        String rangeStr = pathPattern.substring(start + 1, end);
        String formatStr = pathPattern.substring(0, start) + "%s"
                + pathPattern.substring(end + 1, pathPattern.length());

        String startDate = rangeStr.split(SymbolsConstant.MIDDLE_LINE)[0];
        String endDate = rangeStr.split(SymbolsConstant.MIDDLE_LINE)[1];

        try {
            List<String> allDate = listAllDate(startDate, endDate);
            for (String dateStr : allDate) {
                ret.put(dateStr, String.format(formatStr, dateStr));
            }
            return ret;
        } catch (Exception e) {
            ret.put("global", pathPattern);
            return ret;
        }
    }

    private static List<String> listAllDate(String start, String end) throws ParseException {
        SimpleDateFormat dateFormat;
        Long timeStep;
        if (start.length() == yyyyMMdd.length()) {
            dateFormat = new SimpleDateFormat(yyyyMMdd);
            timeStep = UnitConstant.DAY2MILLS;
        } else if (start.length() == yyyyMMddHH.length()) {
            dateFormat = new SimpleDateFormat(yyyyMMddHH);
            timeStep = UnitConstant.HOUR2MILLS;
        } else {
            throw new RuntimeException("unSupport file path");
        }
        long startMills = dateFormat.parse(start).getTime();
        long endMills = dateFormat.parse(end).getTime();
        List<String> dateList = new ArrayList<>();
        for (long i = startMills; i <= endMills; i += timeStep) {
            dateList.add(dateFormat.format(new Date(i)));
        }
        return dateList;
    }
}
