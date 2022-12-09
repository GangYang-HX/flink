package com.bilibili.bsql.mysql.client;

import org.apache.flink.util.ShutdownHookUtil;

import com.bilibili.bsql.common.api.function.RdbRowDataLookupFunction;
import com.bilibili.bsql.mysql.table.BsqlMysqlLookupOptionsEntity;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** SqlClient. */
public class SqlClient {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClient.class);
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final Map<String, SQLClient> CLIENTS;

    static {
        CLIENTS = new ConcurrentHashMap<>(4);
        ShutdownHookUtil.addShutdownHookThread(
                new Thread(
                        () -> {
                            CLIENTS.forEach(
                                    (source, client) -> {
                                        client.close();
                                        LOG.info("close source = {}'s c3p0 client", source);
                                    });
                        }),
                SQLClient.class.getSimpleName(),
                LOG);
    }

    public static synchronized SQLClient getInstance(BsqlMysqlLookupOptionsEntity options)
            throws PropertyVetoException, SQLException {
        SQLClient sqlClient = CLIENTS.get(options.url());
        if (null == sqlClient) {
            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(RdbRowDataLookupFunction.DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
            vo.setWorkerPoolSize(options.tmConnectionPoolSize());
            Vertx vertx = Vertx.vertx(vo);
            ComboPooledDataSource ds = new ComboPooledDataSource();
            ds.setDriverClass(MYSQL_DRIVER);
            ds.setLoginTimeout(30);
            ds.setUnreturnedConnectionTimeout(30);
            ds.setMaxPoolSize(options.tmConnectionPoolSize());
            ds.setJdbcUrl(options.url());
            ds.setUser(options.username());
            ds.setPassword(options.password());
            ds.setMaxIdleTime(options.maxIdleTime());
            LOG.info("maxIdleTime set:{}", options.maxIdleTime());
            ds.setIdleConnectionTestPeriod(options.idleConnectionTestPeriodType());
            LOG.info("idleConnectionTestPeriod set:{}", options.idleConnectionTestPeriodType());
            ds.setPreferredTestQuery("select 1");
            JDBCClient client = JDBCClient.create(vertx, ds);
            LOG.info("create a new sql client! url: {}", options.url());
            CLIENTS.put(options.url(), client);
            return client;
        } else {
            return sqlClient;
        }
    }
}
