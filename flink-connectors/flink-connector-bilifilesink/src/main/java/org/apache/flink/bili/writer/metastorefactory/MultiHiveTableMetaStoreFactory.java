package org.apache.flink.bili.writer.metastorefactory;

import org.apache.flink.bili.writer.metricsetter.TriggerMetricsWrapper;
import org.apache.flink.connectors.hive.HiveTableMetaStoreFactory;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.trace.Trace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/24 7:44 下午
 */
public class MultiHiveTableMetaStoreFactory implements TableMetaStoreFactory {

	private static final Logger LOG = LoggerFactory.getLogger(HiveTableMetaStoreFactory.class);

	private static final long serialVersionUID = 1L;

//	private final JobConfWrapper conf;
	private final String hiveVersion;

	public MultiHiveTableMetaStoreFactory(JobConf conf, String hiveVersion) {
//		this.conf = new JobConfWrapper(conf);
		this.hiveVersion = hiveVersion;
	}

	@Override
	public TableMetaStore createTableMetaStore() throws Exception {
		return new MultiHiveTableMetaStore();
	}

	@Override
	public TableMetaStore getTableMetaStore(TableMetaStore tableMetaStore) throws Exception {
		if (tableMetaStore != null) {
			return tableMetaStore;
		}
		LOG.info("hive metastore reconnect.");
		return this.createTableMetaStore();
	}

	/**
	 *
	 */
	public class MultiHiveTableMetaStore implements TableMetaStore {

		private HiveMetastoreClientWrapper client;

		public MultiHiveTableMetaStore() throws MalformedURLException {
			client = HiveMetastoreClientFactory.create(createHiveConf(), hiveVersion);
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
			return null;
		}

		@Override
		public Optional<Path> getPartition(LinkedHashMap<String, String> partitionSpec) throws Exception {
			return Optional.empty();
		}

		@Override
		public void createOrAlterPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath) throws Exception {

		}

		@Override
		public void createOrAlterPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath, boolean eagerCommit) throws Exception {

		}

		@Override
		public void createIfNotExistPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath, TableMetaStore tableMetaStore, TriggerMetricsWrapper triggerMetricsWrapper, Trace trace) throws Exception {

		}

		@Override
		public void createIfNotExistPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath, TableMetaStore tableMetaStore, TriggerMetricsWrapper triggerMetricsWrapper, Trace trace, boolean eagerCommit) throws Exception {

		}


		@Override
		public void close() throws IOException {
			client.close();
		}

		public void createPartitionIfNotExist(
			LinkedHashMap<String, String> partitionSpec,
			Path partitionPath,
			String database,
			String tableName,
			boolean eagerCommit)
			throws Exception {
			StorageDescriptor sd = client.getTable(database, tableName).getSd();
			Partition partition;
			try {
				partition =
					client.getPartition(
						database, tableName, new ArrayList<>(partitionSpec.values()));
				LOG.warn(
					"The partition {} has existed before current commit,"
						+ " the path is {}, this partition will be altered instead of being created",
					partitionSpec,
					partitionPath);
			} catch (NoSuchObjectException e) {
				CallTimeout<Boolean> callTimeout = new CallTimeout<>(() -> createPartition(sd, partitionSpec, partitionPath, database, tableName,eagerCommit));
				Boolean bool = callTimeout.call();
				if (bool == null) {
					throw new RuntimeException("path: " + partitionPath + " createIfNotExistPartition failed.");
				}
				LOG.info("Committed partition {} to metastore", partitionSpec);
			}
		}

		public void createPartitionIfNotExist(
			LinkedHashMap<String, String> partitionSpec,
			Path partitionPath,
			String database,
			String tableName)
			throws Exception {
			createPartitionIfNotExist(partitionSpec, partitionPath, database, tableName, false);
		}

		private boolean createPartition(StorageDescriptor sd, LinkedHashMap<String, String> partSpec, Path path, String database,
									 String tableName, boolean eagerCommit)
			throws Exception {
			StorageDescriptor newSd = new StorageDescriptor(sd);
			newSd.setLocation(path.toString());
			HashMap<String, String> map = new HashMap<>();
			if (eagerCommit) {
				//eagerlycommit的分区创建的时候加入这个commit=false的参数
				map.put("commit", "false");
			}else {
				map.put("commit", "true");
			}
			Partition partition =
				HiveTableUtil.createHivePartition(
					database,
					tableName,
					new ArrayList<>(partSpec.values()),
					newSd,
					map);
			partition.setValues(new ArrayList<>(partSpec.values()));
			try {
				client.add_partition(partition);
				LOG.info("eager commit partition :{},current commit is {}",eagerCommit,map.get("commit"));
			} catch (AlreadyExistsException e) {
				alterPartition(newSd,partSpec,path,partition,database,tableName);
				LOG.info("normal commit partition :{},current commit is {}",eagerCommit,map.get("commit"));
			}
			return true;
		}

		private void alterPartition(
			StorageDescriptor sd,
			LinkedHashMap<String, String> partitionSpec,
			Path partitionPath,
			Partition currentPartition,
			String database,
			String tableName)
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


	}
}
