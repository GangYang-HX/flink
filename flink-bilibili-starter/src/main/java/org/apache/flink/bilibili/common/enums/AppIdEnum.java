package org.apache.flink.bilibili.common.enums;

public enum AppIdEnum {

	KEEPER("datacenter.keeper.keeper", "keeper的appId"),
	SHIELD("datacenter.shielder.shielder", "shield的appId")
	;


	AppIdEnum(String appId, String desc) {
		this.appId = appId;
		this.desc = desc;
	}

	private final String appId;

	private final String desc;

	public String getAppId() {
		return appId;
	}

	public String getDesc() {
		return desc;
	}
}
