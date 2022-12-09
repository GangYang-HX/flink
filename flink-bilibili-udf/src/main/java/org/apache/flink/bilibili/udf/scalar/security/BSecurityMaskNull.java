package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataNullRule;
import org.apache.flink.table.functions.ScalarFunction;

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
