package org.apache.flink.bili.writer.commitpolicy;

import org.apache.flink.bili.writer.metastorefactory.MultiHiveTableMetaStoreFactory;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/28 6:32 下午
 */
public class MultiMetaStoreCommitPolicy implements PartitionCommitPolicy {
	private MultiHiveTableMetaStoreFactory.MultiHiveTableMetaStore multiHiveTableMetaStore;

	public MultiMetaStoreCommitPolicy(MultiHiveTableMetaStoreFactory multiHiveTableMetaStoreFactory) throws Exception {
		this.multiHiveTableMetaStore = (MultiHiveTableMetaStoreFactory.MultiHiveTableMetaStore)
			multiHiveTableMetaStoreFactory.createTableMetaStore();
	}
	public MultiMetaStoreCommitPolicy(){
	}



	@Override
	public void commit(Context context) throws Exception {
		this.multiHiveTableMetaStore.
			createPartitionIfNotExist
				(context.partitionSpec(), context.partitionPath(), context.databaseName(), context.tableName(), context.eagerCommit());
	}

	public boolean checkPartitionSize(int index, int limit) throws IllegalArgumentException {
		if (index > limit) {
			throw new IllegalArgumentException("multi hive metastore create too many partitions，limit " + limit + " once.");
		}
		return true;
	}

	public void setMultiHiveTableMetaStore(MultiHiveTableMetaStoreFactory.MultiHiveTableMetaStore multiHiveTableMetaStore) {
		this.multiHiveTableMetaStore = multiHiveTableMetaStore;
	}
}
