package com.bilibili.bsql.rdb.tableinfo;

import com.bilibili.bsql.common.SideTableInfo;
import com.bilibili.bsql.common.global.SymbolsConstant;
import lombok.Data;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static com.bilibili.bsql.rdb.tableinfo.RdbConfig.*;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RdbSideTableInfo.java
 * @description This is the description of RdbSideTableInfo.java
 * @createTime 2020-10-20 13:34:00
 */
@Data
public class RdbSideTableInfo extends SideTableInfo implements Serializable {

	private String url;
	private String userName;
	private String password;
	private Integer connectionPoolSize;
	private String sqlCondition;
	private String tableName;

	public RdbSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		this.url = helper.getOptions().get(BSQL_URL);
		this.userName = helper.getOptions().get(BSQL_UNAME);
		this.password = helper.getOptions().get(BSQL_PSWD);
		this.tableName = helper.getOptions().get(BSQL_TABLE_NAME);
		this.connectionPoolSize = helper.getOptions().get(BSQL_CONNECTION_POOL_SIZE);

		checkArgument(StringUtils.isNotEmpty(url), "维表:" + getName() + "没有设置url属性");
		checkArgument(StringUtils.isNotEmpty(tableName), "维表:" + getName() + "没有设置tableName属性");
		checkArgument(StringUtils.isNotEmpty(userName), "维表:" + getName() + "没有设置userName属性");
		checkArgument(StringUtils.isNotEmpty(password), "维表:" + getName() + "没有设置password属性");
	}

	public String getSqlCondition(int[][] keys) {
		String sqlCondition = "select ${selectField} from ${tableName} where ";
		List<FieldInfo> physicalFields = getJoinOnFields(keys);
		for (int i = 0; i < physicalFields.size(); i++) {
			String equalField = physicalFields.get(i).getName();
			sqlCondition += equalField + "=? ";
			if (i != physicalFields.size() - 1) {
				sqlCondition += " and ";
			}
		}
		sqlCondition = sqlCondition
				.replace("${tableName}", this.tableName)
				.replace("${selectField}", getPhysicalFields().stream().map(FieldInfo::getName).collect(Collectors.joining(SymbolsConstant.COMMA)));
		return sqlCondition;
	}

	private List<FieldInfo> getJoinOnFields(int[][] keys) {
		List<FieldInfo> joinField = Lists.newArrayList();
		for (int[] key : keys) {
			joinField.add(getPhysicalFields().get(key[0]));
		}
		return joinField;
	}

}
