package com.bilibili.bsql.hdfs.internal;

import com.bilibili.bsql.common.utils.HdfsFileSystem;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zhangyang
 * @Date:2020/5/19
 * @Time:10:52 AM
 */
public class ContinuousReader implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(ContinuousReader.class);
    private FilePartition partition;
    private int activeFileIndex;
    private Reader reader;
    private Seekable seekStream;
    private FileSourceProperties sourceProperties;
    private HdfsFileSystem fs;
    private volatile boolean isEnd = false;

    public ContinuousReader(FileSourceProperties sourceProperties, FilePartition partition) {
        this.sourceProperties = sourceProperties;
        this.partition = partition;
    }

    public void open() throws Exception {
        if (CollectionUtils.isEmpty(partition.getFileList())) {
            LOG.warn("index:{} allocate empty file");
            return;
        }
        fs = new HdfsFileSystem.Builder().build(sourceProperties.getConfDir(), sourceProperties.getUserName());
        switchFile(true);
        if (reader == null) {
            throw new RuntimeException("open file error");
        }
    }

    public List<String> readLines(int num) throws IOException {
        if (reader == null) {
            LOG.warn("reader is null");
            return Collections.EMPTY_LIST;
        }
        String line;
        List<String> lines = new ArrayList<>(num);
        while ((line = reader.br.readLine()) != null) {
            lines.add(line);
            if (lines.size() == num) {
                break;
            }
        }
        if (line == null) {
            switchFile(false);
        }
        return lines;
    }

    private void switchFile(boolean init) throws IOException {
        if (isEnd) {
            return;
        }
        long offset;
        if (init) {
            activeFileIndex = partition.getFileIndex();
            offset = partition.getFileOffset();
        } else {
            activeFileIndex++;
            offset = 0;
        }

        if (activeFileIndex >= partition.getFileList().size()) {
            isEnd = true;
            return;
        }
        String curFile = partition.getFileList().get(activeFileIndex);
        FSDataInputStream inputStream = fs.open(curFile);
        InputStream newInputStream = CodecFactory.wrap(curFile, inputStream);
        seekStream = (Seekable) newInputStream;

        // 压缩类型无法支持seek
        if (offset > 0) {
            if (seekStream instanceof CompressionInputStream) {
                LOG.warn("CompressionInputStream not support seek operation");
            } else {
                seekStream.seek(offset);
            }
        }
        reader = new Reader(new BufferedReader(new InputStreamReader(newInputStream), 64 * 1024), newInputStream);
        LOG.info("start read file:{},offset:{}", curFile, offset);
    }

    @Override
    public void close() {
        if (fs != null) {
            try {
                fs.close();
                fs = null;
            } catch (Exception e) {
                LOG.warn("close fs error", e);
            }
        }
    }

    class Reader implements Closeable {

        BufferedReader br;
        InputStream in;

        public Reader(BufferedReader br, InputStream in) {
            this.br = br;
            this.in = in;
        }

        @Override
        public void close() {
            try {
                br.close();
                in.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public int getActiveFileIndex() {
        return activeFileIndex;
    }

    public long getActiveFileOffset() {
        try {
            return seekStream.getPos();
        } catch (IOException e) {
            LOG.error("get active file offset error");
            return 0L;
        }
    }

    public boolean isEnd() {
        return isEnd;
    }
}
