/** Bilibili.com Inc. Copyright (c) 2009-2019 All Rights Reserved. */

package com.bilibili.bsql.format.delimit.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** DelimitStringUtils. */
public final class DelimitStringUtils {

    private DelimitStringUtils() {}

    public static char unicodeStringDecodeChar(String input) {
        if (input.length() == 1) {
            return input.charAt(0);
        } else {
            StringBuilder retBuf = new StringBuilder();
            int maxLoop = input.length();
            for (int i = 0; i < maxLoop; i++) {
                if (input.charAt(i) == '\\') {
                    boolean isOk =
                            (i < maxLoop - 5)
                                    && ((input.charAt(i + 1) == 'u')
                                            || (input.charAt(i + 1) == 'U'));
                    if (isOk) {
                        try {
                            retBuf.append(
                                    (char) Integer.parseInt(input.substring(i + 2, i + 6), 16));
                            i += 5;
                        } catch (NumberFormatException localNumberFormatException) {
                            retBuf.append(input.charAt(i));
                        }
                    } else if (i < maxLoop - 1) {
                        retBuf.append(input.charAt(++i));
                    }
                } else {
                    retBuf.append(input.charAt(i));
                }
            }

            String afterTrans = retBuf.toString();
            return afterTrans.charAt(0);
        }
    }

    public static String unicodeStringDecode(String unicodeStr) {
        if (unicodeStr == null) {
            return null;
        }
        StringBuilder retBuf = new StringBuilder();
        int maxLoop = unicodeStr.length();
        for (int i = 0; i < maxLoop; i++) {

            if (unicodeStr.charAt(i) == '\\') {
                boolean isOk =
                        (i < maxLoop - 5)
                                && ((unicodeStr.charAt(i + 1) == 'u')
                                        || (unicodeStr.charAt(i + 1) == 'U'));
                if (isOk) {
                    try {
                        retBuf.append(
                                (char) Integer.parseInt(unicodeStr.substring(i + 2, i + 6), 16));
                        i += 5;
                    } catch (NumberFormatException localNumberFormatException) {
                        retBuf.append(unicodeStr.charAt(i));
                    }
                } else if (i < maxLoop - 1) {
                    retBuf.append(unicodeStr.charAt(++i));
                }
            } else {
                retBuf.append(unicodeStr.charAt(i));
            }
        }
        return retBuf.toString();
    }

    /**
     * replace with position, include from,exclude end,all index start from 0.
     *
     * @param originalSql
     * @param from
     * @param end
     * @param newStr
     * @return
     */
    public static String replace(String originalSql, int from, int end, String newStr) {
        if (StringUtils.isEmpty(originalSql)) {
            return originalSql;
        }
        String frontPart = originalSql.substring(0, from + 1);
        String behindPart = originalSql.substring(end, originalSql.length());
        return frontPart + newStr + behindPart;
    }

    /**
     * replace with position, include from,exclude end,all index start from 0.
     *
     * @param sqlSplit
     * @param fromLine
     * @param fromLinePos
     * @param endLine
     * @param endLinePos
     * @param newStr
     * @return
     */
    public static String[] replace(
            String[] sqlSplit,
            int fromLine,
            int fromLinePos,
            int endLine,
            int endLinePos,
            String newStr) {

        if (sqlSplit.length == 0) {
            return sqlSplit;
        }
        String frontPart = sqlSplit[fromLine].substring(0, fromLinePos + 1);
        String behindPart = sqlSplit[endLine].substring(endLinePos, sqlSplit[endLine].length());
        String newLine = frontPart + newStr + behindPart;
        List<String> ret = new ArrayList<>();
        for (int i = 0; i < sqlSplit.length; i++) {
            if (i < fromLine) {
                ret.add(sqlSplit[i]);
            } else if (i > endLine) {
                ret.add(sqlSplit[i]);
            } else {
                ret.add(newLine);
                i = endLine + 1;
            }
        }
        String[] arrayRet = new String[ret.size()];
        for (int i = 0; i < ret.size(); i++) {
            arrayRet[i] = ret.get(i);
        }
        return arrayRet;
    }

    public static List<String> replace(
            List<String> sqlSplit,
            int fromLine,
            int fromLinePos,
            int endLine,
            int endLinePos,
            String newStr) {
        String[] newSqlSplit = new String[sqlSplit.size()];
        for (int i = 0; i < sqlSplit.size(); i++) {
            newSqlSplit[i] = sqlSplit.get(i);
        }
        return Arrays.asList(
                replace(newSqlSplit, fromLine, fromLinePos, endLine, endLinePos, newStr));
    }

    /**
     * 提取数字部分.
     *
     * @param str
     * @return
     */
    public static String extractNumber(String str) {
        if (StringUtils.isEmpty(str)) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (char cha : str.toCharArray()) {
            if (Character.isDigit(cha)) {
                stringBuilder.append(cha);
            }
        }
        return stringBuilder.toString();
    }
}
