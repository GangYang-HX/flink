package com.bilibili.bsql.hbase.tableinfo;

import com.bilibili.bsql.common.SideTableInfo;
import com.bilibili.bsql.hbase.util.HBaseTableSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @author zhuzhengjun
 * @date 2020/11/12 4:52 下午
 */
@Data
public class HbaseSideTableInfo extends SideTableInfo implements Serializable {
    public static final String CURR_TYPE = "hbase";


    public static final String TABLE_NAME_KEY = "tableName";
    public static final String HBASE_ZOOKEEPER_QUORUM = "zookeeperQuorum";
    public static final String ZOOKEEPER_PARENT = "zookeeperParent";
    public static final String NULL_STRING_LITERAL = "nullStringLiteral";


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
    public static final ConfigOption<String> HBASE_NULL_STRING_LITERAL = ConfigOptions
            .key("nullStringLiteral")
            .stringType()
            .defaultValue("null")
            .withDescription("Representation for null values for string fields. HBase source and " +
                    "sink encodes/decodes empty bytes as null values for all types except string type.");


    public HbaseSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        super.setType(CURR_TYPE);

        TableSchema tableSchema = context.getCatalogTable().getSchema();
        this.hBaseTableSchema = HBaseTableSchema.fromTableSchema(tableSchema);
        this.tableName = helper.getOptions().get(TABLE_NAME);
        this.host = helper.getOptions().get(ZOOKEEPER_QUORUM);
        this.parent = helper.getOptions().get(ZOOKEEPER_ZNODE_PARENT);
        this.nullStringLiteral = helper.getOptions().get(HBASE_NULL_STRING_LITERAL);

        checkArgument(StringUtils.isNotEmpty(tableName), "hbase side table can not be empty");
        checkArgument(StringUtils.isNotEmpty(host), "hbase side zk_quorum can not be empty");
        checkArgument(StringUtils.isNotEmpty(parent), "hbase side zk_parent can not be empty");
    }

    private String tableName;
    private String host;
    private String parent;
    private String nullStringLiteral;
    private HBaseTableSchema hBaseTableSchema;
}
