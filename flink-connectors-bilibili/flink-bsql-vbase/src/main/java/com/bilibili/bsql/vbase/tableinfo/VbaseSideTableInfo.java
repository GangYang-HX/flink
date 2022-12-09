package com.bilibili.bsql.vbase.tableinfo;

import com.bilibili.bsql.common.SideTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.bilibili.bsql.vbase.tableinfo.VbaseConfig.*;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @author zhuzhengjun
 * @date 2020/10/30 4:37 下午
 */
@Data
public class VbaseSideTableInfo extends SideTableInfo implements Serializable {


    private static final String CURR_TYPE = "vbase";

    private final String host;

    private final String port;

    private final String parent;

    private boolean preRowKey = false;

    private final String tableName;

    private List<String> columnRealNameList = Lists.newArrayList();

    private Map<String, FieldInfo> aliasNameRef = Maps.newHashMap();

    private final String rowkeyName;

    private final String timeOutMs;

    public VbaseSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
        super(helper, context);
        super.setType(CURR_TYPE);
        this.host = helper.getOptions().get(BSQL_VBASE_HOST);
        this.port = helper.getOptions().get(BSQL_VBASE_PORT);
        this.parent = helper.getOptions().get(BSQL_VBASE_PARENT);
        this.tableName = helper.getOptions().get(BSQL_VBASE_TABLENAME);
        this.rowkeyName = helper.getOptions().get(BSQL_VBASE_ROWKEYNAME);
        this.timeOutMs = helper.getOptions().get(BSQL_VBASE_TIMEOUTMS);
        checkArgument(StringUtils.isEmpty(tableName), "维表:" + getName() + "没有设置tableName属性");
        checkArgument(StringUtils.isEmpty(host), "维表:" + getName() + "没有设置zookeeperQuorum属性");
        checkArgument(StringUtils.isEmpty(parent), "维表:" + getName() + "没有设置zookeeperParent属性");
    }

    public void putAliasNameRef(String aliasName, FieldInfo hbaseField) {
        aliasNameRef.put(aliasName, hbaseField);
    }

    public Map<String, FieldInfo> getAliasNameRef() {
        return aliasNameRef;
    }

    public FieldInfo getHbaseField(String fieldAlias) {
        return aliasNameRef.get(fieldAlias);
    }

    public String[] getColumnRealNames() {
        return columnRealNameList.toArray(new String[columnRealNameList.size()]);
    }

    public void addColumnRealName(String realName) {
        this.columnRealNameList.add(realName);
    }

    @Data
    public static class FieldInfo implements Serializable {

        private String cf;

        private String name;

        public FieldInfo(String cf, String name) {
            this.cf = cf;
            this.name = name;
        }
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(cf);
            stringBuilder.append(StringUtils.isEmpty(cf)?"":":");
            stringBuilder.append(name);
            return stringBuilder.toString();
        }
    }


}
