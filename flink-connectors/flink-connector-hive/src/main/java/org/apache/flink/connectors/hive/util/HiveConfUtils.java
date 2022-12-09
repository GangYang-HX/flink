/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive.util;

import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.getHadoopConfiguration;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Utils to create HiveConf, see FLINK-20913 for more information. */
public class HiveConfUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HiveConfUtils.class);

    /**
     * Create HiveConf instance via Hadoop configuration. Since {@link
     * HiveConf#HiveConf(org.apache.hadoop.conf.Configuration, java.lang.Class)} will override
     * properties in Hadoop configuration with Hive default values ({@link org.apache
     * .hadoop.hive.conf.HiveConf.ConfVars}), so we should use this method to create HiveConf
     * instance via Hadoop configuration.
     *
     * @param conf Hadoop configuration
     *
     * @return HiveConf instance
     */
    public static HiveConf create(Configuration conf) {
        HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
        // to make sure Hive configuration properties in conf not be overridden
        hiveConf.addResource(conf);
        return hiveConf;
    }

    public static HiveConf createHiveConf(
            @Nullable String hiveConfDir,
            @Nullable String hadoopConfDir) {
        Configuration hadoopConf = null;
        if (isNullOrWhitespaceOnly(hadoopConfDir)) {
            for (String possibleHadoopConfPath :
                    HadoopUtils.possibleHadoopConfPaths(
                            new org.apache.flink.configuration.Configuration())) {
                hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
                if (hadoopConf != null) {
                    break;
                }
            }
        } else {
            hadoopConf = getHadoopConfiguration(hadoopConfDir);
            if (hadoopConf == null) {
                String possiableUsedConfFiles =
                        "core-site.xml | hdfs-site.xml | yarn-site.xml | mapred-site.xml";
                throw new CatalogException(
                        "Failed to load the hadoop conf from specified path:" + hadoopConfDir,
                        new FileNotFoundException(
                                "Please check the path none of the conf files ("
                                        + possiableUsedConfFiles
                                        + ") exist in the folder."));
            }
        }
        if (hadoopConf == null) {
//            hadoopConf = new Configuration();
            hadoopConf = org.apache.flink.runtime.util.HadoopUtils.getHadoopConfiguration(new org.apache.flink.configuration.Configuration());
        }
        // ignore all the static conf file URLs that HiveConf may have set
        HiveConf.setHiveSiteLocation(null);
        HiveConf.setLoadMetastoreConfig(false);
        HiveConf.setLoadHiveServer2Config(false);
        HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);

        // /Users/yanggang/Downloads/conf /data/service/spark/conf
//        hiveConfDir = (StringUtils.isEmpty(hiveConfDir) ? "/data/service/spark/conf" : hiveConfDir);
//        if (!new File(hiveConfDir).exists()) {
//            hiveConfDir = System.getenv("HADOOP_CONF_DIR") + File.separator;
//        }
        LOG.info("Setting hive conf dir as {}", hiveConfDir);

        if (hiveConfDir != null) {
            Path hiveSite = new Path(hiveConfDir, "hive-site.xml");
            if (!hiveSite.toUri().isAbsolute()) {
                // treat relative URI as local file to be compatible with previous behavior
                hiveSite = new Path(new File(hiveSite.toString()).toURI());
            }
            try (InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite)) {
                hiveConf.addResource(inputStream, hiveSite.toString());
                // trigger a read from the conf so that the input stream is read
                isNullOrWhitespaceOnly(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
            } catch (IOException e) {
                throw new CatalogException(
                        "Failed to load hive-site.xml from specified path:" + hiveSite, e);
            }
        } else {
            // user doesn't provide hive conf dir, we try to find it in classpath
            URL hiveSite =
                    Thread.currentThread().getContextClassLoader().getResource("hive-site.xml");
            if (hiveSite != null) {
                LOG.info("Found {} in classpath: {}", "hive-site.xml", hiveSite);
                hiveConf.addResource(hiveSite);
            }
        }
        return hiveConf;
    }
}
