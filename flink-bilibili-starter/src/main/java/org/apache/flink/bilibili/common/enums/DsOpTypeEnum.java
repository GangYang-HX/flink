package org.apache.flink.bilibili.common.enums;

/**
 * @author : luotianran
 * @version V1.0
 * @Description:
 * @date Date : 2021年11月22日
 */
public enum DsOpTypeEnum {
	WRITE("写"),
	READ("读")
	;

	DsOpTypeEnum(String desc) {
		this.desc = desc;
	}

	//描述
	private String desc;


}
