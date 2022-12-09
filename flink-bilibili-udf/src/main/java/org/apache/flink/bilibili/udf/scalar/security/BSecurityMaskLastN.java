package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataMaskRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 后n位字符掩盖
 * @author xushaungshaung
 * @description 后n位字符转成掩码*
 * @date 2021/11/5
 **/
public class BSecurityMaskLastN extends ScalarFunction {

	public String eval(String str, int n){
		return DataMaskRule.maskSubLogic(str, 0, str.length() - n, str.length(), str.length());
	}
}
