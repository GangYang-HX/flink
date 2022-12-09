package org.apache.flink.table.runtime.operators.join.latency.compress;

/**
 * @author zhangyang
 * @Date:2019/12/11
 * @Time:10:59 AM
 */
public enum CompressEnums {
    GZIP("gzip", 0),
	SNAPPY("snappy", 1),
	NONE("none", 2),
	FAST_GZIP("fast_gzip", 3),
	LZ4("lz4", 4);

    private String name;
    private int    code;

    CompressEnums(String name, int code) {
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
