package org.apache.flink.bilibili.catalog.client;

import com.bapis.datacenter.service.keeper.*;
import io.grpc.ManagedChannel;
import org.apache.flink.bilibili.common.enums.AppIdEnum;
import org.apache.flink.bilibili.common.enums.DsOpTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pleiades.component.env.v1.Env;
import pleiades.component.rpc.client.ChannelBuilder;
import pleiades.component.rpc.client.naming.RPCNamingClientNameResolverFactory;

import java.util.concurrent.TimeUnit;

public class KeeperRpc extends AbstractDatacenterRpcCli {

	private final static Logger LOG = LoggerFactory.getLogger(KeeperRpc.class);

	private ManagedChannel managedChannel;

	private static KeeperRpc keeperRpc;

	private KeeperMultiDatasourceGrpc.KeeperMultiDatasourceBlockingStub keeperMultiDatasourceBlockingStub = null;
	//	private KeeperMultiDatabaseGrpc.KeeperMultiDatabaseBlockingStub keeperMultiDatabaseBlockingStub = null;
//	private KeeperMultiTableGrpc.KeeperMultiTableBlockingStub keeperMultiTableBlockingStub = null;
//	private KeeperPartitionGrpc.KeeperPartitionBlockingStub keeperPartitionBlockingStub = null;
//	private KeeperRelationGrpc.KeeperRelationBlockingStub keeperRelationBlockingStub = null;
	private KeeperGrpc.KeeperBlockingStub keeperBlockingStub = null;

	/**
	 * 获取keeperRpc单例
	 * @return
	 */
	public static KeeperRpc getInstance() {
		if (keeperRpc == null) {
			synchronized (KeeperRpc.class) {
				if (keeperRpc == null) {
					keeperRpc = new KeeperRpc();
					keeperRpc.open();
				}
			}
		}
		return keeperRpc;
	}

	@Override
	public void open() {

		super.open();

		this.managedChannel = ChannelBuilder.forTarget(AppIdEnum.KEEPER.getAppId())
			.directExecutor()
			.disableRetry()
			.defaultLoadBalancingPolicy("round_robin")
			.usePlaintext()
			.keepAliveTime(1, TimeUnit.SECONDS)
			.keepAliveTimeout(1, TimeUnit.SECONDS)
			.idleTimeout(1, TimeUnit.MINUTES)
			.nameResolverFactory(new RPCNamingClientNameResolverFactory(Env.getZone(), NamingResolver.getKeeperResolver()))
			.build();

		keeperMultiDatasourceBlockingStub = KeeperMultiDatasourceGrpc.newBlockingStub(managedChannel);
//		keeperMultiDatabaseBlockingStub = KeeperMultiDatabaseGrpc.newBlockingStub(managedChannel);
//		keeperMultiTableBlockingStub = KeeperMultiTableGrpc.newBlockingStub(managedChannel);
//		keeperPartitionBlockingStub = KeeperPartitionGrpc.newBlockingStub(managedChannel);
//		keeperRelationBlockingStub = KeeperRelationGrpc.newBlockingStub(managedChannel);
		keeperBlockingStub = KeeperGrpc.newBlockingStub(managedChannel);

	}

	@Override
	public void close() {
		if (this.managedChannel != null) {
			managedChannel.shutdownNow();
		}
	}

	/**
	 * 获取数据源的配置信息，如写模式的链接信息
	 * @param dsId keeper维护的数据源id
	 * @param dbType 数据源类型
	 * @return DatasourceConfig 数据源的配置
	 */
	public DatasourceConfig getDsConfigsByDsIdAndUser(String dsId, DbType dbType, DsOpTypeEnum dsOpTypeEnum) {
		//使用的写账号
		String writeUserName = "";
		switch (dbType) {
			case Mysql:
			case TiDB:
				if (DsOpTypeEnum.WRITE.equals(dsOpTypeEnum)) {
					writeUserName = "sqoop_write";
				} else {
					writeUserName = "sqoop";
				}
				break;
			case Clickhouse:
				if (DsOpTypeEnum.WRITE.equals(dsOpTypeEnum)) {
					writeUserName = "write";
				} else {
					writeUserName = "read";
				}
				break;
		}
		//构建rpc请求
		DsAndUserReq req = DsAndUserReq.newBuilder()
			.setDsId(dsId)
			.setUser(writeUserName)
			.build();
		//调用keeper数据源管理的rpc接口
		return keeperMultiDatasourceBlockingStub.getConfigsByDsIdAndUser(req);
	}

	/**
	 * 获取表
	 *
	 * @return
	 */
	public TableDto getTable(String dataSource, String databaseName, String tableName) {

		MetaRequest request = MetaRequest.newBuilder()
			.setDatabaseName(databaseName)
			.setDbType(getDbType(dataSource))
			.setTableName(tableName)
			.setDataServiceName(dataSource)
			.build();

		return keeperBlockingStub.getTableInfo(request);
	}

	/**
	 * table exist
	 *
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public boolean tableExist(String dataSource, String databaseName, String tableName) {
		//该方式经过确认，并不支持所有的数据源类型，故不太准确
		MetaRequest request = MetaRequest.newBuilder()
			.setDatabaseName(databaseName)
			.setDbType(getDbType(dataSource))
			.setTableName(tableName)
			.setDataServiceName(dataSource)
			.build();

		ResponseBool responseBool = keeperBlockingStub.tableExisted(request);
		return responseBool.getResponse();
		//构建rpc入参
//		MetaRequest request = MetaRequest.newBuilder()
//			.setDatabaseName(databaseName)
//			.setDbType(getDbType(dataSource))
//			.setTableName(tableName)
//			.setDataServiceName(dataSource)
//			.build();
//		//调用keeper rpc接口
//		TableDto tableInfo = keeperBlockingStub.getTableInfo(request);
//		return Objects.isNull(tableInfo);
	}

	/**
	 * 根据 catalog name 获取 db type
	 *
	 * @param dataSource
	 * @return
	 */
	public static DbType getDbType(String dataSource) {
		String dataType = dataSource.split("_")[0].toLowerCase();
		switch (dataType) {
			case "hive1":
				return DbType.Hive;
			case "app":
				return DbType.App;
			case "boss":
				return DbType.Boss;
			case "clickhouse":
				return DbType.Clickhouse;
			case "databus":
				return DbType.Databus;
			case "kfc":
				return DbType.Kfc;
			case "es":
				return DbType.ElasticSearch;
			case "hbase":
				return DbType.HBase;
			case "hdfs":
				return DbType.Hdfs;
			case "http":
				return DbType.Http;
			case "kafka":
				return DbType.Kafka;
			case "mysql":
				return DbType.Mysql;
			case "redis":
				return DbType.Redis;
			case "servers":
				return DbType.Servers;
			case "taishan":
				return DbType.Taishan;
			case "tidb":
				return DbType.TiDB;
			case "web":
				return DbType.Web;
			default:
				return DbType.Unknown;
		}
	}


}
