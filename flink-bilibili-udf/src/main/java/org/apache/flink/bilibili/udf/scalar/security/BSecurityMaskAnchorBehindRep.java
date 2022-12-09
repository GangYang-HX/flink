package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataMaskRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 锚点后字符掩盖
 * @author xushaungshaung
 * @description 锚点anchor后字符串转成掩码*
 * @date 2021/11/5
 **/
public class BSecurityMaskAnchorBehindRep extends ScalarFunction {

	public String eval(String str, String anchor){
		return DataMaskRule.maskAnchorBehind(str, anchor);
	}
}
