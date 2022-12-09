package com.bilibili.bsql.hdfs.tableinfo;

import com.bilibili.bsql.common.SideTableInfo;
import com.bilibili.bsql.hdfs.util.HiveMetaWrapper;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bilibili.bsql.hdfs.tableinfo.HdfsConfig.BSQL_HDFS_USER;
import static com.bilibili.bsql.hdfs.tableinfo.HdfsConfig.FORMAT;
import static com.bilibili.bsql.hdfs.tableinfo.HiveConfig.*;
import static org.apache.flink.table.utils.SaberStringUtils.unicodeStringDecode;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @Author: JinZhengyu
 * @Date: 2022/7/27 下午9:06
 */
@Data
public class HiveSideTableInfo extends SideTableInfo implements Serializable {


    private final static Logger LOG = LoggerFactory.getLogger(HiveSideTableInfo.class);

    public static final String CUR_TYPE = "hdfs";
    public static final String FILED_DELIMITER = "fieldDelim";
    public static final String TABLE_LOCATION = "location";
    private static final String DEFAULT_UNICODE_TAB = "\u0009";
    public static final String TABLE_FORMAT_TEXT = "text";
    public static final String TABLE_FORMAT_ORC = "orc";
    public static final String TABLE_FORMAT_PARQUET = "parquet";
    private static final String TABLE_PARTITION_KEY = "partitionKey";
    private String hdfsUser;
    private String tableName;
    private String delimiter;
    private List<String> partitionKeys;
    private String joinKey;
    private String location;
    private String format;
    private boolean checkSuccessFile;
    private int failoverTimes;
    private boolean useMem;
    private boolean dqcCheck;
    private boolean turnOnJoinLog;


    /**
     * wrapper tableInfo
     *
     * @param helper
     * @param context
     */
    public HiveSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        super.setType(CUR_TYPE);
        CatalogTable catalogTable = context.getCatalogTable();
        List<TableColumn> tableColumns = catalogTable.getSchema().getTableColumns();
        Map<String, String> options = catalogTable.getOptions();
        for (TableColumn column : tableColumns) {
            LOG.info("catalog ======= column:" + column.getName());
        }
        for (Map.Entry<String, String> entry : options.entrySet()) {
            LOG.info("catalog ======= options key:{}  value:{} ", entry.getKey(), entry.getValue());
        }
        ReadableConfig helperOptions = helper.getOptions();
        this.hdfsUser = helperOptions.get(BSQL_HDFS_USER);
        this.tableName = helperOptions.get(BSQL_HIVE_TABLE_NAME);
        this.joinKey = helperOptions.get(BSQL_HIVE_JOIN_KEY);
        checkArgument(StringUtils.isNotBlank(joinKey), "joinKey can not be empty.");
        boolean joinKeyExisted = tableColumns.stream().anyMatch(e -> e.getName().equals(joinKey));
        if (!joinKeyExisted) {
            throw new RuntimeException(String.format("The table:%s fields does not contain the joinKey field:%s"
                    , tableName, joinKey));
        }
        this.useMem = helperOptions.get(BSQL_USE_MEM);
        this.turnOnJoinLog = helperOptions.get(BSQL_JOIN_LOG);
/*
		//对接catalog本地无法调试，debug请将该注释打开，使用封装好的HiveMetaWrapper的rewrite方法进行本地调试
		try {
			String[] split =this.tableName.split("\\.");
			HiveMetaWrapper.rewrite(options, split[0], split[1]);
		} catch (Exception e) {
			throw new RuntimeException("Can not rewrite hive props.",e);
		}
		if (StringUtils.isNotBlank(options.get(TABLE_PARTITION_KEY))){
			partitionKeys = Arrays.stream(options.get(TABLE_PARTITION_KEY).split(";")).collect(Collectors.toList());
		}
*/
        if (StringUtils.isNotBlank(options.get(TABLE_PARTITION_KEY))) {
            partitionKeys = Arrays.stream(options.get(TABLE_PARTITION_KEY).split(",")).collect(Collectors.toList());
        }
        this.location = options.get(TABLE_LOCATION);
        this.format = options.get(FORMAT);
        if (format.equals(TABLE_FORMAT_TEXT)) {
            String fieldDelim = options.get(FILED_DELIMITER);
            this.delimiter = ("\\t".equals(fieldDelim) || "\t".equals(fieldDelim)) ? DEFAULT_UNICODE_TAB : unicodeStringDecode(fieldDelim);
        }
        LOG.info("tableName:{},tableType:{},partitionKeys:{},location:{}", tableName, format, partitionKeys == null ? "" : String.join(",", partitionKeys), location);
    }

}
