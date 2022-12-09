package org.apache.flink.kubernetes.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** HadoopFetcher. */
public class HadoopFetcher implements Fetcher {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopFetcher.class);
    private static final String ENV_FLINK_HOME_DIR = "FLINK_HOME";
    private static final String ENV_FLINK_HOME_DIR_DEFAULT = "/opt/flink";
    private static final String KEYTAB = "security.kerberos.login.keytab";
    public static final String CUSTOM_LIB_SUFFIX = "custom-lib";
    private static final String PRINCIPAL = "security.kerberos.login.principal";
    private static final String JARS = "pipeline.jars";
    private Configuration hadoopConf;
    private static List<String> userJars;
    private String userLibLocalDir;

    public static void main(String[] args) {
        try {
            HadoopFetcher hadoopFetcher = new HadoopFetcher();
            hadoopFetcher.initialize(args);
            hadoopFetcher.fetch(userJars);
        } catch (Exception e) {
            LOG.error("Download error", e);
            System.exit(250);
        }
    }

    void initialize(String[] args) throws IOException {
        hadoopConf = createHadoopConf(args);
        String keytabFileKey = hadoopConf.get(KEYTAB);
        String principal = hadoopConf.get(PRINCIPAL);
        UserGroupInformation.setConfiguration(hadoopConf);
        if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation.loginUserFromKeytab(principal, keytabFileKey);
        }
        userJars = getUserJarFiles(hadoopConf);
        userLibLocalDir = initUserLibLocalDir();
    }

    @Override
    public void fetch(List<String> userJars) {
        userJars.stream()
                .map(jarFile -> new Path(jarFile))
                .forEach(
                        path -> {
                            try {
                                LOG.info("Start downloading file：" + path);
                                FileSystem fileSystem = path.getFileSystem(hadoopConf);
                                /** Copy it a file from a remote filesystem to the local one */
                                fileSystem.copyToLocalFile(
                                        path, new Path(userLibLocalDir, path.getName()));
                                LOG.info(path + " has been downloaded successfully.");
                            } catch (IOException e) {
                                LOG.info("Failed to download " + path, e);
                                throw new RuntimeException(e);
                            }
                        });
    }

    public static String initUserLibLocalDir() {
        String basePath =
                System.getenv().getOrDefault(ENV_FLINK_HOME_DIR, ENV_FLINK_HOME_DIR_DEFAULT);
        File dir = new File(basePath, CUSTOM_LIB_SUFFIX);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        return dir.getPath();
    }

    public List<String> getUserJarFiles(Configuration conf) {
        final List<String> expectedSchemes = new ArrayList<>();
        expectedSchemes.add("hdfs");
        expectedSchemes.add("viewfs");
        return Arrays.stream(conf.get(JARS).split(";"))
                .map(
                        uncheckedFunction(
                                path -> {
                                    URI uri = resolveURI(path);
                                    if (expectedSchemes.contains(uri.getScheme())
                                            && uri.isAbsolute()) {
                                        LOG.info(path + " was successfully found.");
                                        return path;
                                    }
                                    throw new IllegalArgumentException(
                                            "Only hdfs/viewfs is supported.");
                                }))
                .collect(Collectors.toList());
    }

    public static Configuration createHadoopConf(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        LOG.info("HADOOP_CONF_DIR: " + hadoopConfDir);
        addHadoopConfIfFound(configuration, hadoopConfDir);
        LOG.info("Dynamic params: " + args);
        GenericOptionsParser parser = new GenericOptionsParser(configuration, args);
        return parser.getConfiguration();
    }

    private static void addHadoopConfIfFound(
            Configuration configuration, String localHadoopConfigurationDirectory) {
        final List<String> expectedFileNames = new ArrayList<>();
        expectedFileNames.add("core-site.xml");
        expectedFileNames.add("hdfs-site.xml");
        expectedFileNames.add("mount-table.xml");

        final File directory = new File(localHadoopConfigurationDirectory);
        if (directory.exists() && directory.isDirectory()) {
            Arrays.stream(directory.listFiles())
                    .filter(
                            file ->
                                    file.isFile()
                                            && expectedFileNames.stream()
                                                    .anyMatch(name -> file.getName().equals(name)))
                    .forEach(file -> configuration.addResource(new Path(file.getPath())));
        }
    }

    private static URI resolveURI(String path) throws URISyntaxException {
        final URI uri = new URI(path);
        if (uri.getScheme() != null) {
            return uri;
        }
        return new File(path).getAbsoluteFile().toURI();
    }

    public void setUserLibLocalDir(String userLibLocalDir) {
        this.userLibLocalDir = userLibLocalDir;
    }

    public void setHadoopConf(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public static <A, B> Function<A, B> uncheckedFunction(
            FunctionWithException<A, B, ?> functionWithException) {
        return (A value) -> {
            try {
                return functionWithException.apply(value);
            } catch (Throwable t) {
                rethrow(t);
                // we need this to appease the compiler :-(
                return null;
            }
        };
    }

    public static void rethrow(Throwable t) {
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else {
            throw new RuntimeException(t);
        }
    }

    interface FunctionWithException<T, R, E extends Throwable> {
        R apply(T value) throws E;
    }
}
