package com.bilibili.bsql.common.enums;

public enum TableType {
	//源表
	SOURCE(1),
	//目的表
	SINK(2),
	// 维度表
	SIDE(3);

	int type;

	TableType(int type) {
		this.type = type;
	}

	public int getType() {
		return type;
	}
}
