package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataSubstrRule;

/**
 * 第x-y位截取
 * @author xushaungshaung
 * @description 截取第x位到第y位
 * @date 2021/11/5
 **/
public class BSecuritySubRep extends ScalarFunction {

	public String eval(String str, int x, int y){
		return DataSubstrRule.subStr(str, x - 1, y);
	}
}
