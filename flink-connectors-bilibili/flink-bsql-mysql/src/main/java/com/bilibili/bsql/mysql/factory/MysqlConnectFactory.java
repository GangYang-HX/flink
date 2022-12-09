package com.bilibili.bsql.mysql.factory;

import com.bilibili.bsql.common.utils.JDBCLock;
import com.bilibili.bsql.mysql.tableinfo.MysqlSideTableInfo;
import com.bilibili.bsql.rdb.function.RdbRowDataLookupFunction;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.util.ShutdownHookUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MysqlConnectFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlConnectFactory.class);
    private final static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private final static Map<String, SQLClient> SQL_CLIENTS;

    static {
        SQL_CLIENTS = new ConcurrentHashMap<>(4);
		ShutdownHookUtil.addShutdownHookThread(new Thread(() -> SQL_CLIENTS.values().forEach((SQLClient sqlClient) -> {
			LOG.info("Taskmanager(or Jvm) has shutdown then close mysqlClient");
			sqlClient.close();
		})), SQLClient.class.getSimpleName(), LOG);
    }

    public static SQLClient getSqlClient(MysqlSideTableInfo sideTableInfo, int connectionPoolSize) throws Exception {
        String cacheKey = generateKey(sideTableInfo.getUrl());
        SQLClient sqlClient = SQL_CLIENTS.get(cacheKey);
        if (sqlClient == null) {
            // create client
            // 避免多sink多side场景死问题，添加JDBCLock对象全局锁
            synchronized (JDBCLock.class) {
                // double check
                if (SQL_CLIENTS.get(cacheKey) == null) {
                    VertxOptions vo = new VertxOptions();
                    vo.setEventLoopPoolSize(RdbRowDataLookupFunction.DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
                    vo.setWorkerPoolSize(connectionPoolSize);
                    Vertx vertx = Vertx.vertx(vo);
                    ComboPooledDataSource ds = new ComboPooledDataSource();
                    ds.setDriverClass(MYSQL_DRIVER);
                    ds.setLoginTimeout(30);
                    ds.setUnreturnedConnectionTimeout(30);
                    ds.setMaxPoolSize(connectionPoolSize);
                    ds.setJdbcUrl(sideTableInfo.getUrl());
                    ds.setUser(sideTableInfo.getUserName());
                    ds.setPassword(sideTableInfo.getPassword());
                    ds.setMaxIdleTime(sideTableInfo.getMaxIdleTime());
                    LOG.info("maxIdleTime set:{}", sideTableInfo.getMaxIdleTime());
                    ds.setIdleConnectionTestPeriod(sideTableInfo.getIdleConnectionTestPeriod());
                    LOG.info("idleConnectionTestPeriod set:{}", sideTableInfo.getIdleConnectionTestPeriod());
                    ds.setPreferredTestQuery("select 1");
                    JDBCClient client = JDBCClient.create(vertx, ds);
                    SQL_CLIENTS.put(cacheKey, client);
                    LOG.info("create a new sql client! url: {}", sideTableInfo.getUrl());
                    return client;
                }
                return SQL_CLIENTS.get(cacheKey);
            }
        }
        return sqlClient;
    }

    private static String generateKey(String... keys) {
        StringBuilder encodeKey = new StringBuilder();
        Arrays.stream(keys).forEach(key -> encodeKey.append(Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8))));
        return encodeKey.toString();
    }
}
