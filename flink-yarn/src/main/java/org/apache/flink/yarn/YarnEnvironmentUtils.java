package org.apache.flink.yarn;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;

import java.io.File;
import java.util.Map;
import java.util.Random;

/** Utility class that provides modify java.io.tmpdir methods to work with Apache Hadoop YARN. */
public class YarnEnvironmentUtils {

    public static void overwriteProps(Map<String, String> env, Logger log) {
        modifyJavaIoTmpDir(env, log);
    }

    private static void modifyJavaIoTmpDir(Map<String, String> env, Logger log) {
        final String localDirs = env.get(ApplicationConstants.Environment.LOCAL_DIRS.key());
        if (localDirs == null || localDirs.length() == 0) {
            log.warn("No changes have been made to java.io.tmpdir.");
            return;
        }
        String[] localDirArray = localDirs.split(",");
        int i = new Random().nextInt(localDirArray.length);
        String containId = env.get(ApplicationConstants.Environment.CONTAINER_ID.key());
        String javaIoTmpDir = localDirArray[i] + File.separator + containId;
        System.setProperty("java.io.tmpdir", javaIoTmpDir);
        log.info("Modify java.io.tmpdir:{}", javaIoTmpDir);
    }
}
