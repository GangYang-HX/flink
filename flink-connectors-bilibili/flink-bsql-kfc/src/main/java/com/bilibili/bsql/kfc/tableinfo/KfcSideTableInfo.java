/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kfc.tableinfo;

import com.bilibili.bsql.common.SideTableInfo;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 *
 * @author zhouxiaogang
 * @version $Id: RedisSideTableInfo.java, v 0.1 2020-10-13 18:47
zhouxiaogang Exp $$
 */
@Data
public class KfcSideTableInfo extends SideTableInfo implements Serializable {

	public static final String SERVER_URL = "url";
	public static final String ZONE_ADDR = "zone";
	public static final String RETRY_TIMES = "retryTimes";
    public static final String BATCH_TIMEOUT_MILLIS = "batchTimeMillis";
    public static final String BATCH_SIZE = "batchSize";

    private List<String> serviceUrl = Collections.emptyList();
    private String zone;
	private boolean multiKey   = false;

	private Integer keyIndex;
	private Integer valueIndex;
	private Integer retryTimes;
	private long batchTimeMillis = 500;
	private int batchSize = 200;


	public static final ConfigOption<String> SERVER_URL_KEY = ConfigOptions
			.key(SERVER_URL)
			.stringType()
			.noDefaultValue()
			.withDescription("redis url");

	public static final ConfigOption<String> ZONE_ADDR_KEY = ConfigOptions
		.key(ZONE_ADDR)
		.stringType()
		.defaultValue("suzhou")
		.withDescription("generator room address, SuZhou or ChangShu etc..");

	public static final ConfigOption<Integer> RETRY_TIMES_KEY = ConfigOptions
		.key(RETRY_TIMES)
		.intType()
		.defaultValue(3)
		.withDescription("retry times");

    public static final ConfigOption<Integer> BATCH_TIMEOUT_MILLIS_KEY = ConfigOptions
            .key(BATCH_TIMEOUT_MILLIS)
            .intType()
            .defaultValue(500)
            .withDescription("batch timeout millis");

    public static final ConfigOption<Integer> BATCH_SIZE_KEY = ConfigOptions
            .key(BATCH_SIZE)
            .intType()
            .defaultValue(200)
            .withDescription("batch size");

	public KfcSideTableInfo(FactoryUtil.TableFactoryHelper helper, DynamicTableFactory.Context context) {
		super(helper, context);

		this.batchTimeMillis = helper.getOptions().get(BATCH_TIMEOUT_MILLIS_KEY);
		this.batchSize = helper.getOptions().get(BATCH_SIZE_KEY);
		this.type = "kfc";
		String urls = helper.getOptions().get(SERVER_URL_KEY);
		if(StringUtils.isNotEmpty(urls)){
            String[] urlList = urls.split(";");
            this.serviceUrl = Arrays.asList(urlList);
        }
		this.zone = helper.getOptions().get(ZONE_ADDR_KEY);
		checkArgument(this.zone.equalsIgnoreCase("suzhou") || this.zone.equalsIgnoreCase("changshu"),
			"zone only support SuZhou or ChangShu for now");
		List<FieldInfo> physicalFields = getPhysicalFields();

		checkArgument(physicalFields.size() == 2, "kfc only support 2 fields");
		checkArgument(getPrimaryKeyIdx().size() == 1, "kfc only support for one key");
		this.keyIndex = getPrimaryKeyIdx().get(0);
		this.valueIndex = 1 - this.keyIndex;

		checkArgument(getPhysicalFields().get(keyIndex).getType().getTypeRoot() == VARCHAR,
				"kfc only support string key");
		checkArgument((multiKeyDelimitor!=null)==(getPhysicalFields().get(valueIndex).getType().getTypeRoot()==ARRAY),
			"when defined delimiter key, value type must be array and vice versa ");

		if (multiKeyDelimitor!=null) {
			checkArgument(getPhysicalFields().get(valueIndex).getType().getTypeRoot() == ARRAY);
			this.multiKey = true;
		}

		retryTimes = helper.getOptions().get(RETRY_TIMES_KEY);
		checkArgument(retryTimes != null, "retryTimes must not bu null");
	}
}
