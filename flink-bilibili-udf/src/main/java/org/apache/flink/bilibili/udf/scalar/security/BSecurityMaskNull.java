package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataNullRule;

/**
 * 置null
 * @author xushaungshaung
 * @description 置null
 * @date 2021/11/5
 **/
public class BSecurityMaskNull extends ScalarFunction {

	public String eval(String str){
		return DataNullRule.maskNull(str);
	}
}
