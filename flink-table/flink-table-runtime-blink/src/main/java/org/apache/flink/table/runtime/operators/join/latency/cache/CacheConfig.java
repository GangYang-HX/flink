package org.apache.flink.table.runtime.operators.join.latency.cache;

/**
 * @author zhangyang
 * @Date:2019/12/13
 * @Time:11:46 AM
 */
public class CacheConfig {

    private String address;
    private int    type;
    private long   bucketNum;
    private long   maxCacheSize;
    private int    compressType;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(long bucketNum) {
        this.bucketNum = bucketNum;
    }

	public long getMaxCacheSize() { return maxCacheSize; }

	public void setMaxCacheSize(long maxCacheSize) { this.maxCacheSize = maxCacheSize; }


    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getCompressType() {
        return compressType;
    }

    public void setCompressType(int compressType) {
        this.compressType = compressType;
    }
}
