package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataSubstrRule;

/**
 * 截取前n位置
 * @author xushaungshaung
 * @description 截取前n位置
 * @date 2021/11/5
 **/
public class BSecuritySubFirstN extends ScalarFunction {

	public String eval(String str, int n){
		return DataSubstrRule.subStr(str, 0, n);
	}}
