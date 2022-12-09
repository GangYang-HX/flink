package org.apache.flink.kubernetes.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/** org.apache.flink.kubernetes.fetcher.TestHadoopFetcher. */
public class TestHadoopFetcher {
    static MiniDFSCluster hdfsCluster;
    static FileSystem fileSystem;
    static Path basePath;
    static Path pathA;
    static Path pathB;

    @BeforeClass
    public static void init() throws Exception {
        createHDFS();

        pathA = new Path(basePath, "a.test");
        pathB = new Path(basePath, "b.test");

        fileSystem.createNewFile(pathA);
        fileSystem.createNewFile(pathB);

        Configuration conf = fileSystem.getConf();
        String jarsKey = "pipeline.jars";
        conf.set(jarsKey, pathA + ";" + pathB);

        fileSystem.setConf(conf);
    }

    @Test
    public void testFetch() {
        HadoopFetcher fetcher = new HadoopFetcher();
        fetcher.setHadoopConf(fileSystem.getConf());
        List<String> jarFiles = fetcher.getUserJarFiles(fileSystem.getConf());
        String localBasePath = System.getProperty("java.io.tmpdir") + "userlib";
        fetcher.setUserLibLocalDir(localBasePath);

        fetcher.fetch(jarFiles);
        File userLib = new File(localBasePath);

        Assert.assertTrue(userLib.exists());
        Assert.assertTrue(
                Arrays.stream(userLib.listFiles())
                        .anyMatch(file -> file.getName().equals(pathA.getName())));
        Assert.assertTrue(
                Arrays.stream(userLib.listFiles())
                        .anyMatch(file -> file.getName().equals(pathB.getName())));
    }

    @Test
    public void testInitUserLibLocalDir() {
        String dir = HadoopFetcher.initUserLibLocalDir();
        String basePath = System.getProperty("java.io.tmpdir");
        Assert.assertTrue(new File(dir).exists());
        Assert.assertEquals(basePath + "userlib", dir);
    }

    @Test
    public void testGetUserJarFiles() {
        HadoopFetcher fetcher = new HadoopFetcher();
        List<String> jarFiles = fetcher.getUserJarFiles(fileSystem.getConf());
        Assert.assertTrue(jarFiles.size() == 2);
    }

    public static void createHDFS() throws Exception {
        final File baseDir = new File("/tmp");
        Configuration hdConf = new Configuration();
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

        final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
        hdfsCluster = builder.build();

        fileSystem = hdfsCluster.getFileSystem();
        basePath = new Path(fileSystem.getUri() + "/test/jars/");
    }

    @AfterClass
    public static void destroyHDFS() throws Exception {
        if (hdfsCluster != null) {
            hdfsCluster.getFileSystem().delete(new Path(basePath.toUri()), true);
            hdfsCluster.shutdown();
        }
    }
}
