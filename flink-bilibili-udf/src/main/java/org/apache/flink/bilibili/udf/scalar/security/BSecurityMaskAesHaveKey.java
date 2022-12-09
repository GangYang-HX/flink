package org.apache.flink.bilibili.udf.scalar.security;

import org.apache.flink.table.functions.ScalarFunction;

import com.bilibili.security.tool.DataEncryptRule;

/**
 * aes加密,默认key
 * @author xushaungshaung
 * @description AES加密_默认key
 * @date 2021/11/5
 **/
public class BSecurityMaskAesHaveKey extends ScalarFunction {

	private String anchor = "jkl;POIU1234++==";


	public String eval(String str){
		try {
			return DataEncryptRule.maskAes(str, anchor);
		} catch (Exception var5) {
			var5.printStackTrace();
			return null;
		}
	}
}
