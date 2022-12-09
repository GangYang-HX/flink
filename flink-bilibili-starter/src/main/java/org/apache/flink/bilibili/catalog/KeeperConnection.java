package org.apache.flink.bilibili.catalog;

import com.bapis.datacenter.service.keeper.*;
import io.grpc.ManagedChannel;
import io.grpc.NameResolver;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.bilibili.enums.DsOpTypeEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pleiades.component.env.v1.Env;
import pleiades.component.rpc.client.ChannelBuilder;
import pleiades.component.rpc.client.naming.RPCNamingClientNameResolverFactory;
import pleiades.venus.naming.client.NamingClient;
import pleiades.venus.naming.client.Namings;
import pleiades.venus.naming.client.resolve.NamingResolver;
import pleiades.venus.naming.discovery.DiscoveryNamingClient;
import pleiades.venus.naming.discovery.event.DiscoveryEventPublisher;
import pleiades.venus.naming.discovery.transport.DiscoveryTransportFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KeeperConnection implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KeeperConnection.class);

    private static final String KEEP_APP_ID = "datacenter.keeper.keeper";
    private static final String HIVE_TABLE_DB_TYPE = "hive1";
    private static final String HUDI_TABLE_DB_TYPE = "hudi";

    private static NamingResolver namingResolver;

    private final ManagedChannel managedChannel;

    private final KeeperGrpc.KeeperBlockingStub keeperBlockingStub;

    private final KeeperMultiDatasourceGrpc.KeeperMultiDatasourceBlockingStub keeperMultiDatasourceBlockingStub;

    private static KeeperConnection keeperConnection;

    static {
        initEnv();
        initNamingResolver();
    }

    public static KeeperConnection getInstance() {
        if (keeperConnection == null) {
            synchronized (KeeperConnection.class) {
                if (keeperConnection == null) {
                    keeperConnection = new KeeperConnection();
                }
            }
        }
        return keeperConnection;
    }

    private KeeperConnection() {
        NameResolver.Factory resolverFactory =
                new RPCNamingClientNameResolverFactory(Env.getZone(), namingResolver);
        this.managedChannel =
                ChannelBuilder.forTarget(KEEP_APP_ID)
                        .directExecutor()
                        .defaultLoadBalancingPolicy("round_robin")
                        .usePlaintext()
                        .keepAliveTime(10, TimeUnit.SECONDS)
                        .keepAliveTimeout(1, TimeUnit.SECONDS)
                        .idleTimeout(1, TimeUnit.MINUTES)
                        .nameResolverFactory(resolverFactory)
                        .build();
        keeperBlockingStub = KeeperGrpc.newBlockingStub(managedChannel);
        keeperMultiDatasourceBlockingStub = KeeperMultiDatasourceGrpc.newBlockingStub(managedChannel);
    }

    public void close() {
        if (this.managedChannel != null) {
            managedChannel.shutdownNow();
        }
    }

    /**
     * 获取表
     *
     * @return
     */
    public TableDto getTable(String dataSource, String databaseName, String tableName) {

        MetaRequest request =
                MetaRequest.newBuilder()
                        .setDatabaseName(databaseName)
                        .setDbType(getDbType(dataSource))
                        .setTableName(tableName)
                        .setDataServiceName(dataSource)
                        .build();

        return keeperBlockingStub.getTableInfo(request);
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
     * table exist
     *
     * @param databaseName
     * @param tableName
     * @return
     */
    public boolean tableExist(String dataSource, String databaseName, String tableName) {

        MetaRequest request =
                MetaRequest.newBuilder()
                        .setDatabaseName(databaseName)
                        .setDbType(getDbType(dataSource))
                        .setTableName(tableName)
                        .setDataServiceName(dataSource)
                        .build();

        ResponseBool responseBool = keeperBlockingStub.tableExisted(request);
        return responseBool.getResponse();
    }

    /**
     * 根据 catalog name 获取 db type
     *
     * @param dataSource
     * @return
     */
    public static DbType getDbType(String dataSource) {
        String dataType = dataSource.split("_")[0].toLowerCase();
        for (DbType type : DbType.values()) {
            if (StringUtils.endsWithIgnoreCase(type.name(), dataType)) {
                return type;
            }
            if (HIVE_TABLE_DB_TYPE.equals(dataType) || HUDI_TABLE_DB_TYPE.equals(dataType)) {
                return DbType.Hive;
            }
        }
        return DbType.Unknown;
    }

    private static void initEnv() {
        //discovery 环境变量
        Properties properties = new Properties();
        String deployEnv = System.getenv("DEPLOY_ENV");
        if (StringUtils.isBlank(deployEnv)) {
            deployEnv = "prod";
        }
        properties.setProperty("deploy_env", deployEnv);
        properties.setProperty("discovery_zone", "sh001");
        Env.reload(properties);
        LOG.info("init env success");
    }

    private static void initNamingResolver() {
        if (namingResolver == null) {
            synchronized (KeeperConnection.class) {
                if (namingResolver == null) {
                    DiscoveryEventPublisher eventPublisher = new DiscoveryEventPublisher();
                    NamingClient namingClient =
                            new DiscoveryNamingClient(
                                    new DiscoveryTransportFactory(eventPublisher), eventPublisher);
                    namingClient.start();
                    namingResolver = namingClient.resolveForReady(KEEP_APP_ID, Namings.Scheme.GRPC);
                    LOG.info("init namingResolver success");
                }
            }
        }
    }
}
