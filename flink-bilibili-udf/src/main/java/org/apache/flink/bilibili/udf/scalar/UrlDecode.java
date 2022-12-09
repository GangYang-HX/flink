package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.StringUtils;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * @author zhangyang
 * @Date:2019/11/8
 * @Time:3:37 PM
 */
public class UrlDecode extends ScalarFunction {

    public String eval(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        } else {
            try {
                return URLDecoder.decode(str, StandardCharsets.UTF_8.name());
            } catch (Exception e) {
                // ignore
                return null;
            }

        }
    }
}
