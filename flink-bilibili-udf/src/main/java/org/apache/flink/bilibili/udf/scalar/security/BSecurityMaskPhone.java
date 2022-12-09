package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataMaskRule;
import com.bilibili.security.tool.DataRegexCheckRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 手机号码掩盖
 * @author xushaungshaung
 * @description 将手机号码第x位到第y位的字符转成掩码*
 * 第四位参数：
 * 如果不符合email正则
 * 为0，返回空字符串
 * 为1，返回原字符串
 * @date 2021/11/5
 **/
public class BSecurityMaskPhone extends ScalarFunction {

	public String eval(String str, int x, int y, int type){
		if (DataRegexCheckRule.checkPhone(str)) {
			return DataMaskRule.maskSubLogic(str, 0, x - 1, y, str.length());
		} else {
			return type == 0 ? "" : str;
		}
	}
}
