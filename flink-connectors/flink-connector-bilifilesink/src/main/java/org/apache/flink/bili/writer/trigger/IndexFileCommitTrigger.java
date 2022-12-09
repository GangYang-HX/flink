package org.apache.flink.bili.writer.trigger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;

public class IndexFileCommitTrigger {

    private static final Logger LOG = LoggerFactory.getLogger(IndexFileCommitTrigger.class);

    public final static String VERTICAL_LINE = "|";

    private static final ListStateDescriptor<String> INFLIGHT_INDEX_FILES_STATE_DESC =
            new ListStateDescriptor<>("inflight-index-files", StringSerializer.INSTANCE);

    /**
     * 索引文件所在目录
     */
    private static final String READY_DIR = ".READY";

    /**
     * 正在写的索引文件
     */
    private static final String INFLIGHT_SUFFIX = ".inflight";

    /**
     * 写完并且提交的索引文件
     */
    private static final String COMMIT_SUFFIX = ".commit";

    private final Map<String, List<PartFileStatus>> readyFiles;
    private final Set<String> inflightIndexFiles;

    private final FileSystem fs;

    private final ListState<String> inflightIndexFilesState;

    private final int threadSize = 1;
    private ThreadPoolExecutor executor;

    public IndexFileCommitTrigger(
            boolean isRestored,
            OperatorStateStore stateStore,
            Configuration conf,
            FileSystem fileSystem) throws Exception {
        readyFiles = new HashMap<>();
        inflightIndexFiles = new CopyOnWriteArraySet<>();

        fs = fileSystem;
        inflightIndexFilesState = stateStore.getListState(INFLIGHT_INDEX_FILES_STATE_DESC);

        if (isRestored) {
            for (String state : inflightIndexFilesState.get()) {
                inflightIndexFiles.add(state);
            }
        }
    }

    public void init() {
        int maxTask = 10000;
        executor = new ThreadPoolExecutor(threadSize, threadSize, 2000L, TimeUnit.MICROSECONDS,
                new LinkedBlockingQueue<>(maxTask),
                new ThreadFactoryBuilder().setNameFormat("index-file-committer-%d").build());
    }

    public void close() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    public void append(List<PartFileStatus> files) {

        for (PartFileStatus fileStatus : files) {
            String partitionPath = fileStatus.bucketPath.getPath();

            if (readyFiles.containsKey(partitionPath)) {
                readyFiles.get(partitionPath).add(fileStatus);
            } else {
                readyFiles.put(partitionPath, new ArrayList(Arrays.asList(fileStatus)));
            }
        }
    }

    public void snapshotState() throws Exception {
        writeIndexFile();

        inflightIndexFilesState.clear();
        inflightIndexFilesState.addAll(new ArrayList<>(inflightIndexFiles));
    }

    public void notifyCheckpointComplete() {
        LOG.info("commit inflight index files={}", inflightIndexFiles);

        for (String inflight : inflightIndexFiles) {
            executor.submit(() -> {
                try {
                    Path inflightPath = new Path(getUriPathString(inflight));
                    Path commitPath = new Path(getUriPathString(inflight.replace(INFLIGHT_SUFFIX, COMMIT_SUFFIX)));

                    boolean result = fs.rename(inflightPath, commitPath);
                    if (result) {
                        LOG.info("commit index file success, file={}", inflight);
                    } else {
                        LOG.info("file={} has already been committed, ignore", inflight);
                    }

                    inflightIndexFiles.remove(inflight);
                } catch (Exception e) {
                    // 不需要抛出异常，即使失败下一次 snapshot 也会重新 commit
                    LOG.warn("commit index file failed, file={}", inflight, e);
                }
            });
        }
    }

    private void writeIndexFile() throws Exception {
        long current = Clock.systemUTC().millis();
        LOG.info("write index file start, current={}", current);
        if (readyFiles.isEmpty()) {
            return;
        }

        List<Future<Boolean>> futures = new ArrayList<>(readyFiles.size());
        for (Map.Entry<String, List<PartFileStatus>> entry : readyFiles.entrySet()) {
            futures.add(executor.submit(() -> {
                try {
                    // HDFS 打开索引文件，写入内容
                    Path inflightPath = new Path( getUriPathString(entry.getKey()) + "/" + READY_DIR + "/" + current + INFLIGHT_SUFFIX);

                    StringBuilder sb = new StringBuilder();
                    for (PartFileStatus file : entry.getValue()) {
                        // format：filePath|line
                        sb.append(file.getFilePath() + VERTICAL_LINE + file.getEventSize() + "\n");
                    }

                    try (FSDataOutputStream out = fs.create(inflightPath, FileSystem.WriteMode.OVERWRITE);
                         OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                        writer.write(sb.toString());

                        LOG.info("write index file, path={}, content:\n{}", inflightPath, sb.toString());
                        return true;
                    }
                } catch (Throwable e) {
                    throw new RuntimeException("write index file error, path=" + entry.getKey(), e);
                }

            }));
        }

        // 必须保证 get 所有 future 才算完成 snapshot
        boolean result = true;
        Exception exception = null;

        for (Future<Boolean> f : futures) {
            try {
                if (result) {
                    f.get(2, TimeUnit.MINUTES);
                }
            } catch (Exception e) {
                result = false;
                exception = e;
                LOG.error("get future error", e);
            } finally {
                if (!f.isDone()) {
                    f.cancel(true);
                }
            }
        }

        if (result) {
            // 全部写成功后记录 index file，如果在上面各个写线程中记录可能会出现诡异现象
            // 比如：f.get 超时，cancel 线程不一定真的 cancel 掉，索引文件可能记录到 inflightIndexFiles，产生脏索引
            for (String location : readyFiles.keySet()) {
                inflightIndexFiles.add(location + "/" + READY_DIR + "/" + current + INFLIGHT_SUFFIX);
            }
            readyFiles.clear();
        } else {
            throw exception;
        }

        LOG.info("write index file finished, cost={}", Clock.systemUTC().millis() - current);
    }

    private String getUriPathString (String originPath) {
        if (StringUtils.isBlank(originPath)){
            throw new NullPointerException("originPath cannot be null ");
        }

        //uri path string
        return new Path(originPath).getPath();
    }

}