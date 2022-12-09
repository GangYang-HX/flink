package com.bilibili.bsql.hdfs.tableinfo;

import com.bilibili.bsql.common.SourceTableInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.hdfs.tableinfo.HdfsConfig.*;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className HdfsSourceTableInfo.java
 * @description This is the description of HdfsSourceTableInfo.java
 * @createTime 2020-11-04 18:55:00
 */
@Data
public class HdfsSourceTableInfo extends SourceTableInfo {

    public static final String CURR_TYPE = "hdfs";
    private String path;
    private String conf = "";
    private String user = "";
    private String delimiterKey = "|";
    private Integer fetchSize = 2000;
    public transient DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public HdfsSourceTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        setType(CURR_TYPE);
        this.path = helper.getOptions().get(BSQL_HDFS_PATH);
        this.conf = helper.getOptions().get(BSQL_HDFS_CONF);
        this.user = helper.getOptions().get(BSQL_HDFS_USER);
        this.delimiterKey = helper.getOptions().get(BSQL_HDFS_DELIMITER_KEY);
        this.fetchSize = helper.getOptions().get(BSQL_HDFS_FETCH_SIZE);
        this.decodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, BSQL_FORMAT);

        checkArgument(StringUtils.isNotEmpty(path), "source表:" + getName() + "没有填写hdfs的目录");
    }
}
