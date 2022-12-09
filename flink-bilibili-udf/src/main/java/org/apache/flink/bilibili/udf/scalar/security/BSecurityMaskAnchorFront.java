package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataMaskRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 锚点前字符掩盖
 * @author xushaungshaung
 * @description 锚点anchor前字符串转成掩码*
 * @date 2021/11/5
 **/
public class BSecurityMaskAnchorFront extends ScalarFunction {

	public String eval(String a, String b){
		return DataMaskRule.maskAnchorFront(a, b);
	}
}
