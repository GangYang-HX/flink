package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.core.fs.Path;

import java.io.Serializable;

/**
 * @author zhouxiaogang
 * @version $Id: PartFileStatus.java, v 0.1 2020-07-21 13:23 zhouxiaogang Exp $$
 */
public class PartFileStatus<BucketId> implements Serializable {
    public Path filePath;
    public Long offset;
    public Path bucketPath;
    private long eventSize;
    private BucketId bucketId;

    public PartFileStatus(Path filename, Path bucketPath, long eventSize) {
        this.filePath = filename;
        this.bucketPath = bucketPath;
        this.eventSize = eventSize;
    }

    public PartFileStatus(
            Path filename, Path bucketPath, long eventSize, BucketId bucketId, Long offset) {

        this.filePath = filename;
        this.bucketPath = bucketPath;
        this.eventSize = eventSize;
        this.bucketId = bucketId;
        this.offset = offset;
    }

    public void setEventSize(long eventSize) {
        this.eventSize = eventSize;
    }

    public BucketId getBucketId() {
        return bucketId;
    }

    public void setBucketId(BucketId bucketId) {
        this.bucketId = bucketId;
    }

    public Path getFilePath() {
        return filePath;
    }

    public void setFilePath(Path filePath) {
        this.filePath = filePath;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public void setBucketPath(Path bucketPath) {
        this.bucketPath = bucketPath;
    }

    public long getEventSize() {
        return eventSize;
    }

    @Override
    public String toString() {
        return "PartFileStatus{"
                + "filePath="
                + filePath
                + ", offset="
                + offset
                + ", bucketPath="
                + bucketPath
                + ", eventSize="
                + eventSize
                + ", bucketId="
                + bucketId
                + '}';
    }
}
