package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataMaskRule;

/**
 * 显示前x位_后y位
 * @author xushaungshaung
 * @description 显示前x位,后y位,其他字符转成掩码*
 * @date 2021/11/5
 **/
public class BSecurityMaskFirstLastN extends ScalarFunction {

	public String eval(String str, int x ,int y){
		return DataMaskRule.maskFirstLastN(str, x, y);
	}
}
