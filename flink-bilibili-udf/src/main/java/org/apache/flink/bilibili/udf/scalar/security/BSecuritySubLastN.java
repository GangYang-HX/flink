package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataSubstrRule;

/**
 * 截取后n位置
 * @author xushaungshaung
 * @description 截取后n位置
 * @date 2021/11/5
 **/
public class BSecuritySubLastN extends ScalarFunction {

	public String eval(String str, int n){
		return DataSubstrRule.subStr(str, str.length() - n, str.length());
	}}
