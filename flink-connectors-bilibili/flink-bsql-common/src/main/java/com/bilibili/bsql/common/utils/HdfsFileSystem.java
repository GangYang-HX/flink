package com.bilibili.bsql.common.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author galaxy
 */
public class HdfsFileSystem {

    private FileSystem fs;

    private Configuration conf;

    public void close() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private HdfsFileSystem(FileSystem fs, Configuration conf) {
        this.fs = fs;
        this.conf = conf;
    }

    public FileSystem getFs() {
        return fs;
    }

    public Configuration getConf() {
        return conf;
    }

    public void copy(String srcPath, String destPath) throws IOException {
        Preconditions.checkNotNull(fs);

        if (!fs.exists(new Path(srcPath))) {
            throw new IOException("src not exist!");
        }

        // 要排除src=desc的情况
        if (combinePath(srcPath, "").equals(combinePath(destPath, ""))) {
            return;
        }

        Path src = new Path(srcPath);
        Path dest = new Path(destPath);

        org.apache.hadoop.fs.FileUtil.copy(fs, src, fs, dest, false, conf);
    }

    public List<FileStatus> listFileStatus(String basePath) throws IOException {
        return listFileStatus(new Path(basePath));
    }

    public List<FileStatus> listFileStatus(Path basePath) throws IOException {
        Preconditions.checkNotNull(fs);
        if (fs.isFile(basePath)) {
            throw new IOException("not a dict!");
        }
        return Arrays.asList(fs.listStatus(basePath));
    }

    public void delete(String path) throws IOException {
        Preconditions.checkNotNull(path);
        if (path.isEmpty() || path.equals(File.separator)) {
            throw new IOException("error path: " + path);
        }
        if (fs.exists(new Path(path))) {
            fs.delete(new Path(path), true);
        }
    }

    public boolean exist(String path) throws IOException {
        Preconditions.checkNotNull(fs);
        return fs.exists(new Path(path));
    }

    public FSDataInputStream openFile(Path path) throws IOException {
        Preconditions.checkNotNull(fs);
        if (!fs.isFile(path)) {
            throw new IOException("not a file!");
        }
        return fs.open(path);
    }

    public FSDataInputStream openFile(String path) throws IOException {
        return openFile(new Path(path));
    }

    public void mkdirs(String path) throws IOException {
        Preconditions.checkNotNull(fs);
        if (!fs.mkdirs(new Path(path))) {
            throw new IOException("mkdir error");
        }
    }

    public FSDataOutputStream create(Path path) throws IOException {
        return fs.create(path);
    }

    private String combinePath(String parent, String current) {
        return parent + (parent.endsWith(File.separator) ? "" : File.separator) + current;
    }

    /**
     * hdfs文件下载
     *
     * @param hdfsFile
     * @param localFile
     * @throws IOException
     */
    public void download(String hdfsFile, String localFile) throws IOException {
        fs.copyToLocalFile(new Path(hdfsFile), new Path(localFile));
    }

    /**
     * hdfs文件上传
     *
     * @param localFile
     * @param hdfsFile
     * @throws IOException
     */
    public void upload(String localFile, String hdfsFile) throws IOException {
        fs.copyFromLocalFile(new Path(localFile), new Path(hdfsFile));
    }

    /**
     * 查询所有文件
     *
     * @param path
     * @param depth
     * @return
     * @throws IOException
     */
    public List<FileStatus> listFileStatus(String path, int depth) throws IOException {
        List<FileStatus> fileStatusList = new ArrayList<>();
        listFileStatus(new Path(path), fileStatusList, depth);
        return fileStatusList;
    }

    private void listFileStatus(Path path, List<FileStatus> files, int depth) throws IOException {
        if (depth == 0) {
            return;
        }
        FileStatus[] subStatus = fs.listStatus(path);
        for (FileStatus item : subStatus) {
            if (item.isFile()) {
                files.add(item);
            }

            if (item.isDirectory()) {
                listFileStatus(item.getPath(), files, depth - 1);
            }
        }
    }

    public List<FileStatus> listStatus(String path) throws IOException {
        if (StringUtils.isEmpty(path)) {
            return Collections.EMPTY_LIST;
        }
        FileStatus[] allSub = fs.listStatus(new Path(path));
        List<FileStatus> ret = new ArrayList<>(allSub.length);
        for (FileStatus it : allSub) {
            ret.add(it);
        }
        return ret;
    }

    public FSDataInputStream open(String filePath) throws IOException {
        return fs.open(new Path(filePath));
    }

    public static class Builder {

        private final static String DEFAULT_ENV = "/etc/hadoop";
        private final static String DEFAULT_USER_NAME = "hdfs";

        public HdfsFileSystem build(String env, String userName) throws Exception {
            if (StringUtils.isEmpty(env)) {
                env = System.getenv("HADOOP_CONF_DIR");
                if (null == env) {
                    env = DEFAULT_ENV;
                }
            }
            if (!env.endsWith(File.separator)) {
                env = env + File.separator;
            }
            Configuration conf = new Configuration();
            conf.addResource(new Path(env + "core-site.xml"));
            conf.addResource(new Path(env + "hdfs-site.xml"));
            conf.addResource(new Path(env + "mount-table.xml"));

            if (StringUtils.isEmpty(userName)) {
                userName = DEFAULT_USER_NAME;
            }
            FileSystem fs = FileSystem.newInstance(FileSystem.getDefaultUri(conf), conf, userName);
            return new HdfsFileSystem(fs, conf);
        }
    }

}
