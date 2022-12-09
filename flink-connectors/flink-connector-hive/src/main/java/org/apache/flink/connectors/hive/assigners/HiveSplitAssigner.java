package org.apache.flink.connectors.hive.assigners;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Optional;

public class HiveSplitAssigner implements FileSplitAssigner {

    private final ArrayDeque<FileSourceSplit> splits;

    public HiveSplitAssigner(Collection<FileSourceSplit> splits) {
        this.splits = new ArrayDeque(splits);
    }

    // ------------------------------------------------------------------------

    @Override
    public Optional<FileSourceSplit> getNext(String hostname) {
        final int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove());
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> newSplits) {
        splits.addAll(newSplits);
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        return splits;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "SimpleSplitAssigner " + splits;
    }

    private long getLatestFileModifyTime(Path path) {
        try {
            FileSystem fileSystem = path.getFileSystem();
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            return fileStatus.getModificationTime();
        } catch (Exception e) {
            return 0L;
        }
    }
}
