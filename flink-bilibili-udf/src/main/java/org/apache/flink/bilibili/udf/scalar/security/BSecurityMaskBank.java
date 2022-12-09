package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataMaskRule;
import com.bilibili.security.tool.DataRegexCheckRule;

/**
 * 银行卡号掩盖
 * @author xushaungshaung
 * @description 银行卡号第x位到第y位的字符转成掩码*
 * 第四位参数：
 * 如果不符合email正则
 * 为0，返回空字符串
 * 为1，返回原字符串
 * @date 2021/11/5
 **/
public class BSecurityMaskBank extends ScalarFunction {

	public String eval(String str, int x, int y, int type){
		if (DataRegexCheckRule.checkBank(str)) {
			return DataMaskRule.maskSubLogic(str, 0, x - 1, y, str.length());
		} else {
			return type == 0 ? "" : str;
		}
	}
}
