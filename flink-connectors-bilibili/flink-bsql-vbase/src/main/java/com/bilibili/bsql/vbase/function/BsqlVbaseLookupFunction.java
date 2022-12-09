package com.bilibili.bsql.vbase.function;

import com.bilibili.bsql.common.function.AsyncSideFunction;
import com.bilibili.bsql.vbase.tableinfo.VbaseSideTableInfo;
import com.bilibili.bsql.vbase.utils.HBaseReadWriteHelper;
import com.bilibili.bsql.vbase.utils.HBaseTableSchema;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author zhuzhengjun
 * @date 2020/11/4 11:27 上午
 */
public class BsqlVbaseLookupFunction extends AsyncSideFunction<VbaseSideTableInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(BsqlVbaseLookupFunction.class);
    private final String hTableName;
    private transient Connection hConnection;
    private transient HBaseReadWriteHelper readHelper;
    private byte[] serializedConfig;
    private HBaseTableSchema hbaseTableSchema;
    private final String zkQuorum;
    private final String zkParent;
    private transient HTable table;
    private ThreadLocal<Table> tableCache;
    private ExecutorService executorService;

    private transient TableName tableName;


    public BsqlVbaseLookupFunction(VbaseSideTableInfo vbaseSideTableInfo, LookupTableSource.LookupContext context) {

        super(vbaseSideTableInfo, context);
        this.hTableName = vbaseSideTableInfo.getTableName();
        this.zkParent = vbaseSideTableInfo.getParent();
        this.zkQuorum = vbaseSideTableInfo.getHost();

    }

    @Override
    protected void open0(FunctionContext context) throws Exception {
        LOG.info("start open ...");
        Configuration config = getHbaseClient(zkQuorum, zkParent);
        try {
            hConnection = ConnectionFactory.createConnection(config);
            executorService = new ScheduledThreadPoolExecutor(5);
            tableCache = ThreadLocal.withInitial(() -> {
                try {
                    return hConnection.getTable(TableName.valueOf(hTableName));
                } catch (IOException e) {
                    LOG.error("get hbase table error", e);
                }
                return null;
            });
        } catch (TableNotFoundException tnfe) {
            LOG.error("Table '{}' not found ", hTableName, tnfe);
            throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
        } catch (IOException ioe) {
            LOG.error("Exception while creating connection to HBase.", ioe);
            throw new RuntimeException("Cannot create connection to HBase.", ioe);
        }
        this.readHelper = new HBaseReadWriteHelper(hbaseTableSchema);
        LOG.info("end open.");

    }

    //    private org.apache.hadoop.conf.Configuration prepareRuntimeConfiguration() throws IOException {
//        // create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
//        // and overwrite configuration using serialized configuration from client-side env (`hbase-site.xml` in classpath).
//        // user params from client-side have the highest priority
//        org.apache.hadoop.conf.Configuration runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, HBaseConfiguration.create());
//
//        // do validation: check key option(s) in final runtime configuration
//        if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
//            LOG.error("Can not connect to HBase without {} configuration", HConstants.ZOOKEEPER_QUORUM);
//            throw new IOException("Check HBase configuration failed, lost: '" + HConstants.ZOOKEEPER_QUORUM + "'!");
//        }
//
//        return runtimeConfig;
//    }
    private Configuration getHbaseClient(String zkQuorum, String zkParent) {
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "5000");
        config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "5000");
        config.set(HConstants.HBASE_CLIENT_PAUSE, "5");
        config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "2");
        config.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkParent);
        config.set(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, "10");

        return config;
    }


    @Override
    protected void close0() throws Exception {
        LOG.info("start close ...");
        if (null != table) {
            try {
                table.close();
                table = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("exception when close table", e);
            }
        }
        if (null != hConnection) {
            try {
                hConnection.close();
                hConnection = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("exception when close connection", e);
            }
        }
        LOG.info("end close.");
        //关闭线程池
        if (executorService != null) {
            executorService.shutdown();
        }

    }

    @Override
    protected void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        executorService.submit(() -> {


        });


    }

    @Override
    protected Object createCacheKey(Object... inputs) {
        return null;
    }

}
