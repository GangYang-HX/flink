/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.hbase.tableinfo;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.bilibili.bsql.common.SinkTableInfo;
import com.bilibili.bsql.hbase.util.HBaseTableSchema;

import static com.bilibili.bsql.hbase.util.HBaseTableSchema.validatePrimaryKey;
import lombok.Data;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 *
 * @author zhouxiaogang
 * @version $Id: HbaseSinkTableInfo.java, v 0.1 2020-10-28 19:12
zhouxiaogang Exp $$
 */
@Data
public class HbaseSinkTableInfo extends SinkTableInfo implements Serializable {

    public static final String CURR_TYPE  = "hbase";

    public static final String TABLE_NAME_KEY         = "tableName";
    public static final String HBASE_ZOOKEEPER_QUORUM = "zookeeperQuorum";
    public static final String ZOOKEEPER_PARENT       = "zookeeperParent";

    public static final String VERSION_KEY            = "version";
    public static final String SKIP_WAL_KEY           = "skipWal";


    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key(TABLE_NAME_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("The name of HBase table to connect.");

    public static final ConfigOption<String> ZOOKEEPER_QUORUM = ConfigOptions
            .key(HBASE_ZOOKEEPER_QUORUM)
            .stringType()
            .noDefaultValue()
            .withDescription("The HBase Zookeeper quorum.");

    public static final ConfigOption<String> ZOOKEEPER_ZNODE_PARENT = ConfigOptions
            .key(ZOOKEEPER_PARENT)
            .stringType()
            .noDefaultValue()
            .withDescription("The root dir in Zookeeper for HBase cluster.");

    public static final ConfigOption<String> VERSION = ConfigOptions
            .key(VERSION_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("Hbase version.");

    public static final ConfigOption<Boolean> SKIP_WAL = ConfigOptions
            .key(SKIP_WAL_KEY)
            .booleanType()
            .defaultValue(false)
            .withDescription("Hbase skip WAL.");

    private String              tableName;
    private String              host;
    private String              parent;

    private String              version;
    private boolean             skipWal;
    private Integer rowIdx = -1;
    private Integer colIdx = -1;
    private LogicalType versionDataType;

    private Integer rowkeyIdx;
    private HBaseTableSchema hBaseTableSchema;


    public HbaseSinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        setType(CURR_TYPE);

        this.tableName = helper.getOptions().get(TABLE_NAME);
        this.host = helper.getOptions().get(ZOOKEEPER_QUORUM);
        this.parent = helper.getOptions().get(ZOOKEEPER_ZNODE_PARENT);
        this.version = helper.getOptions().get(VERSION);
        this.skipWal = helper.getOptions().get(SKIP_WAL);

        checkArgument(StringUtils.isNotEmpty(tableName), "hbase sink table can not be empty");
        checkArgument(StringUtils.isNotEmpty(host), "hbase sink zk_quorum can not be empty");
        checkArgument(StringUtils.isNotEmpty(parent), "hbase sink zk_parent can not be empty");

        checkArgument(getPrimaryKeyIdx().size() == 1, "hbase sink need one and only one row key");
        this.rowkeyIdx = getPrimaryKeyIdx().get(0);

        TableSchema tableSchema = context.getCatalogTable().getSchema();
        validatePrimaryKey(tableSchema);
        this.hBaseTableSchema = HBaseTableSchema.fromTableSchema(tableSchema);

        String version = helper.getOptions().get(VERSION);
        checkArgument(version == null || version.contains("."),
                "hbase sink version needs to be cf.col format");
        if (version != null) {
            String[] versionSplit = version.split("\\.");
            for (int i = 0; i < getPhysicalFields().size(); i++) {
                FieldInfo fieldInfo = getPhysicalFields().get(i);
                if(fieldInfo.getName().equals(versionSplit[0])) {
                    RowType cfRow = (RowType)fieldInfo.getType();
                    for (int j = 0; j < cfRow.getFields().size(); j++) {
                        RowType.RowField colField = cfRow.getFields().get(j);
                        if(colField.getName().equals(versionSplit[1])) {
                            this.rowIdx = i;
                            this.colIdx = j;
                            this.versionDataType = colField.getType();
                            break;
                        }
                    }
                }
            }
            checkArgument(this.colIdx != -1, "hbase sink version column can not be found");
            checkArgument(versionDataType instanceof VarCharType
                    || versionDataType instanceof IntType
                    || versionDataType instanceof BigIntType, "hbase version need to be String , int or bigint");
        }
    }
}