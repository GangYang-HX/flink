package com.bilibili.bsql.clickhouse.shard.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class UDFMurmur3 {

	public static Integer evaluate(Long a) {
		if (a == null) {
			return null;
		}
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		bb.putLong(a);
		return Murmur3.hash32(bb.array()) & 0x7fffffff;
	}

	public static Integer evaluate(Integer a) {
		if (a == null) {
			return null;
		}
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		bb.putInt(a);
		return Murmur3.hash32(bb.array()) & 0x7fffffff;
	}

	public static Integer evaluate(Short a) {
		if (a == null) {
			return null;
		}
		ByteBuffer bb = ByteBuffer.allocate(2);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		bb.putShort(a);
		return Murmur3.hash32(bb.array()) & 0x7fffffff;
	}

	public static Integer evaluate(Byte a) {
		if (a == null) {
			return null;
		}
		byte[] b = new byte[1];
		b[0] = a;
		return Murmur3.hash32(b) & 0x7fffffff;
	}

	public static Integer evaluate(String a) {
		if (a == null) {
			return null;
		}
		byte[] b = a.getBytes(StandardCharsets.UTF_8);
		return Murmur3.hash32(b) & 0x7fffffff;
	}

}
