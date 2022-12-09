package org.apache.flink.bili.writer.metastorefactory;


import org.apache.commons.collections.MapUtils;
import org.apache.flink.bili.writer.metricsetter.TriggerMetricsWrapper;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.runtime.util.MapUtil;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.trace.Trace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author: zhuzhengjun
 * @date: 2021/7/21 9:12 下午
 */

public class BsqlHiveTableMetaStoreFactory implements TableMetaStoreFactory {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BsqlHiveTableMetaStoreFactory.class);

    //	private final JobConfWrapper conf;
    private final String hiveVersion;
    private final String database;
    private final String tableName;
    private final String format;
    private final String compress;

    public BsqlHiveTableMetaStoreFactory(JobConf conf, String hiveVersion, String database, String tableName) {
//		this.conf = new JobConfWrapper(conf);
        this.hiveVersion = hiveVersion;
        this.database = database;
        this.tableName = tableName;
        this.format = "";
        this.compress = "";
    }

    public BsqlHiveTableMetaStoreFactory(JobConf conf, String hiveVersion, String database, String tableName,
                                         String format, String compress) {
//		this.conf = new JobConfWrapper(conf);
        this.hiveVersion = hiveVersion;
        this.database = database;
        this.tableName = tableName;
        this.format = format;
        this.compress = compress;
    }

    @Override
    public HiveTableMetaStore createTableMetaStore() throws Exception {
        return new HiveTableMetaStore();
    }

    private class HiveTableMetaStore implements TableMetaStore {

        private HiveMetastoreClientWrapper client;
        private StorageDescriptor sd;

        private HiveTableMetaStore() throws TException, MalformedURLException {
            LOG.info("database:{},tablename:{},hiveversion:{}", database, tableName, hiveVersion);
            client = HiveMetastoreClientFactory.create(createHiveConf(), hiveVersion);
            LOG.info("metastore client created:{}", client);
            sd = client.getTable(database, tableName).getSd();
            LOG.info("meta sd created:{}", sd);
        }

        private HiveConf createHiveConf() throws MalformedURLException {
            org.apache.flink.configuration.Configuration flinkConf = new org.apache.flink.configuration.Configuration();
            Configuration conf = HadoopUtils.getHadoopConfiguration(flinkConf);
            String hiveConfDir = "/etc/second-hadoop/";
            if (!new File(hiveConfDir).exists()) {
                hiveConfDir = System.getenv("HADOOP_CONF_DIR") + File.separator;
            }
            HiveConf.setHiveSiteLocation(Paths.get(hiveConfDir, "hive-site.xml").toUri().toURL());
            return new HiveConf(conf, HiveConf.class);
        }

        @Override
        public Path getLocationPath() {
            return new Path(sd.getLocation());
        }

        @Override
        public Optional<Path> getPartition(LinkedHashMap<String, String> partSpec)
            throws Exception {
            try {
                return Optional.of(
                    new Path(
                        client.getPartition(
                            database,
                            tableName,
                            new ArrayList<>(partSpec.values()))
                            .getSd()
                            .getLocation()));
            } catch (NoSuchObjectException ignore) {
                return Optional.empty();
            }
        }

        @Override
        public void createOrAlterPartition(
            LinkedHashMap<String, String> partitionSpec, Path partitionPath) throws Exception {
			createOrAlterPartition(partitionSpec, partitionPath, false);
        }

		@Override
		public void createOrAlterPartition(
			LinkedHashMap<String, String> partitionSpec, Path partitionPath, boolean eagerCommit) throws Exception {
			Partition partition;
			try {
				partition =
					client.getPartition(
						database, tableName, new ArrayList<>(partitionSpec.values()));
			} catch (NoSuchObjectException e) {
				createPartition(partitionSpec, partitionPath, eagerCommit);
				return;
			}
			alterPartition(partitionSpec, partitionPath, partition);
		}

		@Override
		public void createIfNotExistPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath, TableMetaStore tableMetaStore, TriggerMetricsWrapper triggerMetricsWrapper, Trace trace) throws Exception {
			//默认提交不是eagerCommit，属于正常提交，存在参数 "commit" = "true"
			createIfNotExistPartition(partitionSpec, partitionPath, tableMetaStore, triggerMetricsWrapper, trace, false);
		}

		@Override
		public void createIfNotExistPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath, TableMetaStore tableMetaStore, TriggerMetricsWrapper triggerMetricsWrapper, Trace trace, boolean eagerCommit) throws Exception {
			CallTimeout<Boolean> callTimeout = new CallTimeout<>(() -> createPartition(partitionSpec, partitionPath, tableMetaStore, triggerMetricsWrapper, trace, eagerCommit));
			callTimeout.call();
			tableMetaStore.close();

			if (callTimeout.call() == null) {
				trace.traceEvent(System.currentTimeMillis(), 1, "hive.meta-store.ready.fail", partitionPath.getPath(), 0,"HDFS");
				tableMetaStore.close();
				throw new RuntimeException("createIfNotExistPartition failed.");
			}
		}

        private boolean createPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath, TableMetaStore tableMetaStore, TriggerMetricsWrapper triggerMetricsWrapper, Trace trace, boolean eagerCommit) throws Exception {
            TableMetaStore metaStore = getTableMetaStore(tableMetaStore);
            Optional<Path> path = metaStore.getPartition(partitionSpec);
            if (path.isPresent()) {
                LOG.warn(
                    "The partition {} has existed before current commit,"
                        + " the path is {}, this partition will be altered instead of being created",
                    partitionSpec,
                    path);
                triggerMetricsWrapper.failBackWatermark();
                return false;
            }
            createPartition(partitionSpec, partitionPath, eagerCommit);
            trace.traceEvent(System.currentTimeMillis(), 1, "hive.meta-store.ready.success", partitionPath.getPath(), 0,"HDFS");
            LOG.info("Committed partition {} to metastore", partitionSpec);
            return true;
        }

        private void createPartition(LinkedHashMap<String, String> partSpec, Path path, boolean eagerCommit) throws Exception {
            Map<String, String> partitionParam = new HashMap<>();
            StorageDescriptor newSd = new StorageDescriptor(sd);
            newSd.setLocation(path.toString());
			if (eagerCommit) {
				//eagerlycommit的分区创建的时候加入这个commit=false的参数
				partitionParam.put("commit", "false");
			}else {
				partitionParam.put("commit", "true");
			}
            if (format.equals("orc")) {
                newSd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
                newSd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
                newSd.setSerdeInfo(new SerDeInfo("", "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                        Collections.singletonMap("serialization.format", "1")));

                partitionParam.put("orc.compress",compress);
            }
            Partition partition =
                HiveTableUtil.createHivePartition(
                    database,
                    tableName,
                    new ArrayList<>(partSpec.values()),
                    newSd,
                    partitionParam);
            partition.setValues(new ArrayList<>(partSpec.values()));
			try {
				client.add_partition(partition);
				LOG.info("eager commit partition :{},current commit is {}",eagerCommit,partitionParam.get("commit"));
			} catch (AlreadyExistsException e) {
				alterPartition(partSpec,path,partition);
				LOG.info("normal commit partition :{},current commit is {}",eagerCommit,partitionParam.get("commit"));
			}
		}

        private void alterPartition(
            LinkedHashMap<String, String> partitionSpec,
            Path partitionPath,
            Partition currentPartition)
            throws Exception {
            StorageDescriptor partSD = currentPartition.getSd();
            // the following logic copied from Hive::alterPartitionSpecInMemory
            partSD.setOutputFormat(sd.getOutputFormat());
            partSD.setInputFormat(sd.getInputFormat());
            partSD.getSerdeInfo().setSerializationLib(sd.getSerdeInfo().getSerializationLib());
            partSD.getSerdeInfo().setParameters(sd.getSerdeInfo().getParameters());
            partSD.setBucketCols(sd.getBucketCols());
            partSD.setNumBuckets(sd.getNumBuckets());
            partSD.setSortCols(sd.getSortCols());
            partSD.setLocation(partitionPath.toString());
            client.alter_partition(database, tableName, currentPartition);
        }

        @Override
        public void close() {
            client.close();
        }
    }

    class CallTimeout<V> implements Callable<V> {
        private final long timeout = 10_000;
        private Callable callable;

        public CallTimeout(Callable callable) {
            this.callable = callable;
        }

        @Override
        public V call() {
            FutureTask<V> future = new FutureTask<>(callable);
            Thread t = new Thread(future);
            t.start();

            V result = null;
            try {
                result = future.get(timeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                LOG.warn("task time out", e);
            } catch (InterruptedException | ExecutionException e) {
                future.cancel(true);
                LOG.error("task is error", e);
            }
            return result;
        }
    }

    @Override
    public TableMetaStoreFactory.TableMetaStore getTableMetaStore(TableMetaStoreFactory.TableMetaStore tableMetaStore) throws Exception {
        if (tableMetaStore != null) {
            return tableMetaStore;
        }
        LOG.info("hive metastore reconnect.");
        return this.createTableMetaStore();
    }
}
