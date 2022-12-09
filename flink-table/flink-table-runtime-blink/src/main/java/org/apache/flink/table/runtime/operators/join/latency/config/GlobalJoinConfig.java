package org.apache.flink.table.runtime.operators.join.latency.config;

import java.io.Serializable;

/**
 * @author zhangyang
 * @Date:2019/11/7
 * @Time:10:36 AM
 */
public class GlobalJoinConfig implements Serializable {

    private String          jobId;
    private boolean         globalLatency;
    private long            windowLengthMs;
    private long            delayMs;
    private String          redisAddress;
    private int             redisType    = 0;
    private int             compressType = 0;
    private int             outputRowLength;
    private long            bucketNum;
    private long            maxCacheSize;
    private String          mergeSideFunc;
    private boolean         saveTimerKey;
    private int joinNo;
    private long stateTtl;
	private boolean keyedStateTtlEnable;

    public int getJoinNo() {
        return joinNo;
    }

    public void setJoinNo(int joinNo) {
        this.joinNo = joinNo;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public boolean isGlobalLatency() {
        return globalLatency;
    }

    public void setGlobalLatency(boolean globalLatency) {
        this.globalLatency = globalLatency;
    }

    public long getWindowLengthMs() {
        return windowLengthMs;
    }

    public void setWindowLengthMs(long windowLengthMs) {
        this.windowLengthMs = windowLengthMs;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public String getRedisAddress() {
        return redisAddress;
    }

    public void setRedisAddress(String redisAddress) {
        this.redisAddress = redisAddress;
    }

    public int getRedisType() {
        return redisType;
    }

    public void setRedisType(int redisType) {
        this.redisType = redisType;
    }

    public int getOutputRowLength() {
        return outputRowLength;
    }

    public void setOutputRowLength(int outputRowLength) {
        this.outputRowLength = outputRowLength;
    }

    public long getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(long bucketNum) {
        this.bucketNum = bucketNum;
    }

    public long getMaxCacheSize() { return maxCacheSize; }

    public void setMaxCacheSize(long maxCacheSize) { this.maxCacheSize = maxCacheSize; }

    public String getMergeSideFunc() {
        return mergeSideFunc;
    }

    public void setMergeSideFunc(String mergeSideFunc) {
        this.mergeSideFunc = mergeSideFunc;
    }

    public int getCompressType() {
        return compressType;
    }

    public void setCompressType(int compressType) {
        this.compressType = compressType;
    }

	public boolean isSaveTimerKey() {
		return saveTimerKey;
	}

	public void setSaveTimerKey(boolean saveTimerKey) {
		this.saveTimerKey = saveTimerKey;
	}

	public void setStateTtl(long stateTtl) {
		this.stateTtl = stateTtl;
	}

	public long getStateTtl() {
		return stateTtl;
	}

	public void setKeyedStateTtlEnable(boolean keyedStateTtlEnable) {
		this.keyedStateTtlEnable = keyedStateTtlEnable;
	}

	public boolean isKeyedStateTtlEnable() {
		return this.keyedStateTtlEnable;
	}

}
