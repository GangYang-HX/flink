package com.bilibili.bsql.hdfs.tableinfo;

import com.bilibili.bsql.common.SideTableInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static com.bilibili.bsql.hdfs.tableinfo.HdfsConfig.*;
import static org.apache.flink.table.utils.SaberStringUtils.unicodeStringDecode;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HdfsSideTableInfo.java
 * @description This is the description of HdfsSideTableInfo.java
 * @createTime 2020-10-22 21:21:00
 */
@Data
public class HdfsSideTableInfo extends SideTableInfo implements Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(HdfsSideTableInfo.class);
    public static final String CUR_TYPE = "hdfs";
    private static final String DEFAULT_UNICODE_TAB = "\u0009";

    private final String path;
    private final String delimiter;
    private final String period;
    private final long memoryThresholdSize;
    private final String hdfsUser;
	private final int failoverTimes;
	private final boolean checkSuccessFile;

    public HdfsSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        super.setType(CUR_TYPE);
        this.path = helper.getOptions().get(BSQL_HDFS_PATH);
        this.period = helper.getOptions().get(BSQL_HDFS_PERIOD);
        String opDelimiter = helper.getOptions().get(BSQL_HDFS_DELIMITER_KEY);
        this.delimiter = ("\\t".equals(opDelimiter) || "\t".equals(opDelimiter)) ? DEFAULT_UNICODE_TAB : unicodeStringDecode(opDelimiter);
		this.memoryThresholdSize = helper.getOptions().get(BSQL_MEMORY_THRESHOLD_SIZE);
		this.hdfsUser = helper.getOptions().get(BSQL_HDFS_USER);
		this.checkSuccessFile = Boolean.parseBoolean(helper.getOptions().get(BSQL_HDFS_CHECK_SUCCESS));
		// set 1 as default failover value
		this.failoverTimes = helper.getOptions().get(BSQL_FAILOVER_TIMES);
        checkArgument(StringUtils.isNotEmpty(path), "维表:" + getName() + "没有设置path属性");
        checkArgument(failoverTimes >= 0, "failoverTimes不能为负");
        LOG.info("Hdfs path = {}, period = {}, delimiter = {}， failoverTimes = {}， checkSuccessFile={}", path, period, delimiter, failoverTimes, checkSuccessFile);
    }
}
