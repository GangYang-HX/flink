package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataNullRule;

/**
 * 置空字符串
 * @author xushaungshaung
 * @description 置空字符串
 * @date 2021/11/5
 **/
public class BSecurityMaskSpace extends ScalarFunction {

	public String eval(String str){
		return DataNullRule.maskSpace(str);
	}}
