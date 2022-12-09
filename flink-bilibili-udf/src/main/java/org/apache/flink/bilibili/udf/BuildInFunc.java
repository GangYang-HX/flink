package org.apache.flink.bilibili.udf;

import org.apache.flink.bilibili.udf.aggregate.get.GetLong;
import org.apache.flink.bilibili.udf.aggregate.slidingwindowsum.SlidingWindowSum;
import org.apache.flink.bilibili.udf.aggregate.top.Top;
import org.apache.flink.bilibili.udf.aggregate.windowsum.WindowSum;
import org.apache.flink.bilibili.udf.aggregate.windowsumtop.WindowSumTop;
import org.apache.flink.bilibili.udf.aggregate.windowtop.WindowTop;
import org.apache.flink.bilibili.udf.scalar.*;
import org.apache.flink.bilibili.udf.scalar.compress.DZstd;
import org.apache.flink.bilibili.udf.scalar.compress.DZstdByte;
import org.apache.flink.bilibili.udf.scalar.compress.Zstd;
import org.apache.flink.bilibili.udf.scalar.hash.Murmur64;
import org.apache.flink.bilibili.udf.scalar.security.*;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangyang
 * @Date:2019/10/31
 * @Time:11:49 AM
 */
public class BuildInFunc {

	private final static String UNDER_LINE = "_";

	public static final Map<String, Class<? extends UserDefinedFunction>> FUNC_MAP = new HashMap<>(8);

	static {
		addFunc(FromUnixtime.class);
		addFunc(FromUnixtime10.class);
		addFunc(FromUnixtime13.class);
		addFunc(IsNumber.class);
		addFunc(DateReFormat.class);
		addFunc(ToTimestamp.class);
		addFunc(ToTimestampSafe.class);
		addFunc(ToUnixtime.class);
		addFunc(Print.class);
		addFunc(UrlDecode.class);
		addFunc(NowTs.class);
		addFunc(Length.class);
		addFunc(TimestampFormat.class);
		addFunc(Bdecode.class);
		addFunc(GetJsonObject.class);
		addFunc(Split.class);
		addFunc(Location.class);

        addFunc(SlidingWindowSum.class);
        addFunc(WindowSum.class);
        addFunc(WindowTop.class);
        addFunc(WindowSumTop.class);
        addFunc(Top.class);
        addFunc(GetLong.class);
		addFunc(BSecurityMask.class);
		addFunc(BSecurityMaskAes.class);
		addFunc(BSecurityMaskAesHaveKey.class);
		addFunc(BSecurityMaskAnchorBehindRep.class);
		addFunc(BSecurityMaskAnchorFront.class);
		addFunc(BSecurityMaskBank.class);
		addFunc(BSecurityMaskDate.class);
		addFunc(BSecurityMaskEmail.class);
		addFunc(BSecurityMaskFirstLastN.class);
		addFunc(BSecurityMaskFirstN.class);
		addFunc(BSecurityMaskIdcard.class);
		addFunc(BSecurityMaskLastN.class);
		addFunc(BSecurityMaskNull.class);
		addFunc(BSecurityMaskPhone.class);
		addFunc(BSecurityMaskRound.class);
		addFunc(BSecurityMaskShowFirstN.class);
		addFunc(BSecurityMaskShowLastN.class);
		addFunc(BSecurityMaskSpace.class);
		addFunc(BSecurityMaskSubRep.class);
		addFunc(BSecurityMaskZero.class);
		addFunc(BSecuritySubFirstN.class);
		addFunc(BSecuritySubLastN.class);
		addFunc(BSecuritySubRep.class);

		addFunc(Zstd.class);
		addFunc(DZstd.class);
        addFunc(DZstdByte.class);
		addFunc(Murmur64.class);
	}

	private static void addFunc(Class udfClass) {
		FUNC_MAP.put(hump2Line(udfClass.getSimpleName()).toUpperCase(), udfClass);
	}

	private static String hump2Line(String str) {
		String newStr = str.replaceAll("[A-Z]", "_$0").toLowerCase();
		while (newStr.startsWith(UNDER_LINE)) {
			newStr = newStr.substring(1);
		}
		return newStr;
	}
}
