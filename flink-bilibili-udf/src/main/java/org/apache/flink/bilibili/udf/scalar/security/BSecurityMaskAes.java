package org.apache.flink.bilibili.udf.scalar.security;

import com.bilibili.security.tool.DataEncryptRule;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * AES加密
 * @author xushaungshaung
 * @description aes加密,key需要是16个字符
 * @date 2021/11/5
 **/
public class BSecurityMaskAes extends ScalarFunction {

	public String eval(String a,String b){
		try {
			return DataEncryptRule.maskAes(a, b);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
