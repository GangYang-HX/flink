package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataMaskRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * email掩盖
 * @author xushaungshaung
 * @description email@前内容除前1尾1，其他为*
 * @date 2021/11/5
 **/
public class BSecurityMaskEmail extends ScalarFunction {

	public String eval(String str, int n){
		return DataMaskRule.maskEmail(str, n);
	}
}
