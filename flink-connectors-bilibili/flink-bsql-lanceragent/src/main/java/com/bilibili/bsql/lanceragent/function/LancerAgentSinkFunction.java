package com.bilibili.bsql.lanceragent.function;

import com.bilibili.bsql.common.metrics.CustomizeRichSinkFunction;
import com.bilibili.bsql.lanceragent.tableinfo.LancerAgentSinkTableInfo;
import com.bilibili.lancer2.sdk.Lancer2Client;
import com.bilibili.lancer2.sdk.Lancer2ClientBuilder;
import com.bilibili.lancer2.sdk.common.ClientConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.SneakyThrows;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className LancerAgentSinkFunction.java
 * @description This is the description of LancerAgentSinkFunction.java
 * @createTime 2020-11-10 18:38:00
 */
public class LancerAgentSinkFunction extends CustomizeRichSinkFunction<RowData> {
    private final static Logger LOG = LoggerFactory.getLogger(LancerAgentSinkFunction.class);

    private final String logId;
    private final String delimiter;
    private final String[] fieldNames;
    private Lancer2Client lancer2Client;
    /**
     * 每个客户端随机选取/mnt/storage01～/mnt/storage10任意一块磁盘进行数据写入
     */
    private final static String DEFAULT_LOCAL_PATH = "/mnt/storage";
    private final static int MAX_DISK_NUMBER = 10;
    private final static AtomicInteger seq = new AtomicInteger();
    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
    private final DynamicTableSink.DataStructureConverter converter;

    public LancerAgentSinkFunction(LancerAgentSinkTableInfo sinkTableInfo, DynamicTableSink.Context context) {
        this.converter = context.createDataStructureConverter(sinkTableInfo.getPhysicalRowDataType());
        this.logId = sinkTableInfo.getLogId();
        this.delimiter = sinkTableInfo.getDelimiter();
        this.fieldNames = sinkTableInfo.getFieldNames();
    }

    @Override
    public String sinkType() {
        return "LanceAgent";
    }

    @SneakyThrows
	@Override
    public void doOpen(int taskNumber, int numTasks) throws IOException {
        super.doOpen(taskNumber, numTasks);
        ClientConfig config = new ClientConfig();
        String dir = DEFAULT_LOCAL_PATH;
        Random random = new Random();
        int diskNumber = random.nextInt(MAX_DISK_NUMBER) + 1;
        if (diskNumber < MAX_DISK_NUMBER) {
            dir += "0" + diskNumber;
        } else {
            dir += diskNumber;
        }
        String file = createLogFileName(dir, logId);
        LOG.info("lancer2Client use file : {}", file);
        config.setFile(file);
        // 10GB滚动一次
        config.setFileRollSizeMb(10240);
        this.lancer2Client = Lancer2ClientBuilder.newBuilder().setConfig(config).build();
    }

    @Override
    public boolean doInvoke(RowData rowData, Context context) throws Exception {
        Row record = (Row) converter.toExternal(rowData);
        if (null == record) {
            return false;
        }
        if (fieldNames.length != record.getArity()) {
            LOG.error("invalid data,expect record length:{},real record length:{}", fieldNames.length, record.getArity());
            return false;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < record.getArity(); i++) {
            Object fieldContent = record.getField(i);
            stringBuilder.append(fieldContent == null ? "" : fieldContent.toString());
            stringBuilder.append(i != (record.getArity() - 1) ? delimiter : "");
        }
        long start = System.currentTimeMillis();
        boolean isOk = lancer2Client.put(logId, stringBuilder.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Put to lancer cost = {} ms", (System.currentTimeMillis() - start));
        }
        return isOk;
    }

    private String createLogFileName(String parentDir, String logId) {
        String date = formatter.format(new Date());
        String pidString = ManagementFactory.getRuntimeMXBean().getName();
        String pid = pidString.split("@")[0];
        String fileName = parentDir + "/recordio/logId" + logId + "_" + date + "_" + pid + "_" + seq.incrementAndGet() + ".log";
        return fileName;
    }

    @Override
    public void close() throws Exception {
        lancer2Client.close();
        super.close();
    }
}
