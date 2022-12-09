package com.bilibili.bsql.databus.tableinfo;

import com.bilibili.bsql.common.SourceTableInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import static com.bilibili.bsql.databus.tableinfo.BsqlDataBusConfig.*;
import static org.apache.flink.table.utils.SaberStringUtils.unicodeStringDecode;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className DataBusSourceTableInfo.java
 * @description This is the description of DataBusSourceTableInfo.java
 * @createTime 2020-10-22 17:43:00
 */
@Data
public class DataBusSourceTableInfo extends SourceTableInfo {
	public static final String CURR_TYPE = "databus";
	private String topic;
	private String groupId;
	private String appKey;
	private String appSecret;
	private String address;
	private String delimiterKey;
	private Integer partitionNum;
	public transient DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	public DataBusSourceTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);
		super.setType(CURR_TYPE);
		this.topic = helper.getOptions().get(BSQL_DATABUS_TOPIC_KEY);
		this.groupId = helper.getOptions().get(BSQL_DATABUS_GROUPID_KEY);
		this.appKey = helper.getOptions().get(BSQL_DATABUS_APPKEY_KEY);
		this.appSecret = helper.getOptions().get(BSQL_DATABUS_APPSECRET_KEY);
		this.address = helper.getOptions().get(BSQL_DATABUS_ADDRESS_KEY);
		this.delimiterKey = unicodeStringDecode(helper.getOptions().get(BSQL_DATABUS_DELIMITER_KEY));
		this.decodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, BSQL_FORMAT);
		this.partitionNum = helper.getOptions().get(BSQL_DATABUS_PARTITION_NUM);

		checkArgument(StringUtils.isNotEmpty(topic), "source表:" + getName() + "没有填写topic属性");
		checkArgument(StringUtils.isNotEmpty(groupId), "source表:" + getName() + "没有填写groupId属性");
		checkArgument(StringUtils.isNotEmpty(appKey), "source表:" + getName() + "没有填写appKey属性");
		checkArgument(StringUtils.isNotEmpty(appSecret), "source表:" + getName() + "没有填写appSecret属性");
		checkArgument(StringUtils.isNotEmpty(address), "source表:" + getName() + "没有填写address属性");
	}
}
