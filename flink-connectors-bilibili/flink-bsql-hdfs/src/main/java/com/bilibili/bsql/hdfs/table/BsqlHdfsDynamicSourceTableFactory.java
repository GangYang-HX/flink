package com.bilibili.bsql.hdfs.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSourceFactory;
import com.bilibili.bsql.hdfs.tableinfo.HdfsSourceTableInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Set;

import static com.bilibili.bsql.hdfs.tableinfo.HdfsConfig.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlHdfsDynamicSourceTableFactory.java
 * @description This is the description of BsqlHdfsDynamicSourceTableFactory.java
 * @createTime 2020-11-04 19:04:00
 */
public class BsqlHdfsDynamicSourceTableFactory extends BsqlDynamicTableSourceFactory<HdfsSourceTableInfo> {
    public static final String IDENTIFIER = "bsql-hdfs-source";

    @Override
    public HdfsSourceTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
        return new HdfsSourceTableInfo(helper, context);
    }

    @Override
    public DynamicTableSource generateTableSource(HdfsSourceTableInfo sourceTableInfo) {
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = sourceTableInfo.getDecodingFormat();
        DataType producedDataType = sourceTableInfo.getPhysicalRowDataType();
        return new BsqlHdfsDynamicSource(sourceTableInfo, producedDataType, decodingFormat);
    }

    @Override
    public void setRequiredOptions(Set<ConfigOption<?>> option) {
        option.add(BSQL_HDFS_PATH);
        option.add(BSQL_HDFS_CONF);
        option.add(BSQL_HDFS_USER);
        option.add(BSQL_HDFS_DELIMITER_KEY);
        option.add(BSQL_HDFS_FETCH_SIZE);
    }

    @Override
    public void setOptionalOptions(Set<ConfigOption<?>> option) {

    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
