package com.bilibili.bsql.databus.table;

import com.bilibili.bsql.common.factory.BsqlDynamicTableSourceFactory;
import com.bilibili.bsql.databus.tableinfo.DataBusSourceTableInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Set;

import static com.bilibili.bsql.databus.tableinfo.BsqlDataBusConfig.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlDatabusDynamicSourceFactory.java
 * @description This is the description of BsqlDatabusDynamicSourceFactory.java
 * @createTime 2020-10-22 17:43:00
 */
public class BsqlDataBusDynamicSourceFactory extends BsqlDynamicTableSourceFactory<DataBusSourceTableInfo> {

	public static final String IDENTIFIER = "bsql-databus";

	@Override
	public DataBusSourceTableInfo parseContext(FactoryUtil.TableFactoryHelper helper, Context context) {
		return new DataBusSourceTableInfo(helper, context);
	}

	@Override
	public DynamicTableSource generateTableSource(DataBusSourceTableInfo sourceTableInfo) {
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = sourceTableInfo.getDecodingFormat();
		DataType producedDataType = sourceTableInfo.getPhysicalRowDataType();
		return new BsqlDataBusDynamicSource(sourceTableInfo, decodingFormat, producedDataType);
	}

	@Override
	public void setRequiredOptions(Set<ConfigOption<?>> option) {
		option.add(BSQL_DATABUS_TOPIC_KEY);
		option.add(BSQL_DATABUS_GROUPID_KEY);
		option.add(BSQL_DATABUS_APPKEY_KEY);
		option.add(BSQL_DATABUS_APPSECRET_KEY);
		option.add(BSQL_DATABUS_ADDRESS_KEY);
		option.add(BSQL_DATABUS_DELIMITER_KEY);
		option.add(BSQL_DATABUS_PARTITION_NUM);
	}

	@Override
	public void setOptionalOptions(Set<ConfigOption<?>> option) {

	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
