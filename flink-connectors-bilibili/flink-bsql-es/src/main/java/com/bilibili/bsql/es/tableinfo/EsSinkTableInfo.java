package com.bilibili.bsql.es.tableinfo;

import com.bilibili.bsql.common.SinkTableInfo;
import com.bilibili.bsql.common.utils.ClassUtil;
import com.google.common.base.Preconditions;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.es.tableinfo.EsConfig.*;
import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkArgument;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className EsSinkTableInfo.java
 * @description This is the description of EsSinkTableInfo.java
 * @createTime 2020-10-28 11:14:00
 */
@Data
public class EsSinkTableInfo extends SinkTableInfo {

	private static final String CURR_TYPE = "es";

	private final String address;
	private final String indexName;
	private final String typeName;

	public EsSinkTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		this.address = helper.getOptions().get(BSQL_ES_ADDRESS);
		this.indexName = helper.getOptions().get(BSQL_ES_INDEX_NAME);
		this.typeName = helper.getOptions().get(BSQL_ES_TYPE_NAME);
		setType(CURR_TYPE);

		checkArgument(StringUtils.isNotEmpty(this.address), String.format("sink表:%s 没有设置address属性", getName()));
		checkArgument(StringUtils.isNotEmpty(this.indexName), String.format("sink表:%s 没有设置indexName属性", getName()));
	}

	public Class<?>[] getBizFieldClass() {
		Class<?>[] mappingFieldClass = new Class[getPhysicalFields().size()];
		for (int i = 0; i < mappingFieldClass.length; i++) {
			mappingFieldClass[i] = ClassUtil.stringConvertBizClass(getPhysicalFields().get(i).getType());
		}
		return mappingFieldClass;
	}
}
