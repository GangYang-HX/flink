package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataMaskRule;

/**
 * 第x-y位掩盖
 * @author xushaungshaung
 * @description 第x位到第y位的字符转成掩码*
 * @date 2021/11/5
 **/
public class BSecurityMaskSubRep extends ScalarFunction {

	public String eval(String str, int x, int y){
		return DataMaskRule.maskSubLogic(str, 0, x - 1, y, str.length());
	}

}
