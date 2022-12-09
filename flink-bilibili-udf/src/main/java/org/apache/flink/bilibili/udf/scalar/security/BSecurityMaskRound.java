package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataNullRule;

/**
 * 数值取整
 * @author xushaungshaung
 * @description 前n位保留，后面所有数字置为0
 * @date 2021/11/5
 **/
public class BSecurityMaskRound extends ScalarFunction {

	public int eval(int i, int j){
		return DataNullRule.maskRound(i, j);
	}
}
