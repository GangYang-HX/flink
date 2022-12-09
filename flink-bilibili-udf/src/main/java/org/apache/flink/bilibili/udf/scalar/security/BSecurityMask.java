package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataMaskRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 字符掩盖
 * @author xushaungshaung
 * @description 所有字符转成掩码*
 * @date 2021/11/5
 **/
public class BSecurityMask extends ScalarFunction {

	public String eval(String str){
		return DataMaskRule.maskLogic(str);
	}
}
