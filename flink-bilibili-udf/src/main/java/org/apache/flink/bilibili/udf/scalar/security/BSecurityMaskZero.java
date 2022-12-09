package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataNullRule;

/**
 * 置零
 * @author xushaungshaung
 * @description 置零
 * @date 2021/11/5
 **/
public class BSecurityMaskZero extends ScalarFunction {

	public int eval(int num){
		return DataNullRule.maskZero(num);
	}}
