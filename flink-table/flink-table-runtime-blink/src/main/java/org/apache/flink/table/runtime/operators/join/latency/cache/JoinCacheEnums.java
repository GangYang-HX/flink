package org.apache.flink.table.runtime.operators.join.latency.cache;

/**
 * @author zhangyang
 * @Date:2019/12/11
 * @Time:10:59 AM
 */
public enum JoinCacheEnums {
    SIMPLE("Simple", 0),
	BUCKET("Bucket", 1),
	LETTUCE("Lettuce", 2),
	HASH("Hash", 3),
	ROCKS("Rocks", 4),
	MULTI_BUCKET("Multi_bucket", 5),
	MULTI_SIMPLE("Multi_simple", 6);

    private String name;
    private int    code;

    JoinCacheEnums(String name, int code) {
        this.name = name;
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
