package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataMaskRule;

/**
 * 显示前n位_后字符掩盖
 * @author xushaungshaung
 * @description 显示前n位,后面字符转成掩码*
 * @date 2021/11/5
 **/
public class BSecurityMaskShowFirstN extends ScalarFunction {

	public String eval(String str, int n){
		return DataMaskRule.maskSubLogic(str, 0, n, str.length(), str.length());
	}
}
