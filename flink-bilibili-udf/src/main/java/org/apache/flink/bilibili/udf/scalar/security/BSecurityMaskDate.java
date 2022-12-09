package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataDateRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 日期取整
 * @author xushaungshaung
 * @description 日期取整
 * 第二个参数：
 * 0 月取整，1 日取整，2 时取整，
 * 3 分取整，4 秒取整
 * @date 2021/11/5
 **/
public class BSecurityMaskDate extends ScalarFunction {

	public String eval(String str, int n){
		return DataDateRule.maskDate(str, n);
	}
}
