package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataMaskRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 前n位字符掩盖
 * @author xushaungshaung
 * @description 前n位字符转成掩码*
 * @date 2021/11/5
 **/
public class BSecurityMaskFirstN extends ScalarFunction {

	public String eval(String str, int n){
		return DataMaskRule.maskSubLogic(str, 0, 0, n, str.length());
	}
}
