/**
 * Bilibili.com Inc. Copyright (c) 2009-2019 All Rights Reserved.
 */
package com.bilibili.bsql.common.format;

import com.bilibili.bsql.common.format.converter.CustomRowConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.TimeZone;

import static org.apache.flink.table.utils.SaberStringUtils.unicodeStringDecode;


/**
 * @author zhouxiaogang
 */
public class CustomDelimiterDeserialization extends AbstractDeserializationSchema<RowData> {

	private static final long serialVersionUID = 2385115520960444192L;

	protected static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();
	protected final String[] fieldNames;
	protected final TypeInformation<?>[] fieldTypes;
	protected final String delimiterKey;
	protected final RowType rowType;
	protected final CustomRowConverter converter;


	public CustomDelimiterDeserialization(TypeInformation<RowData> typeInfo, RowType rowType,
										  String delimiterKey) {
		this.fieldNames = ((RowDataTypeInfo) typeInfo).getFieldNames();
		this.fieldTypes = ((RowDataTypeInfo) typeInfo).getFieldTypes();
		this.rowType = rowType;

		this.delimiterKey = unicodeStringDecode(delimiterKey);
		this.converter = new CustomRowConverter(rowType);

	}

	@Override
	public void open(InitializationContext context) throws Exception {
		this.converter.setRowConverterMetricGroup(context.getMetricGroup());
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		String inputString = new String(message);
		String[] inputSplit = StringUtils.splitPreserveAllTokens(inputString, delimiterKey);

		if (inputSplit == null) {
			throw new IOException("kafka deserialize failure");
		}
		return converter.deserializeString(inputSplit);
	}


}
