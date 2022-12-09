package com.bilibili.bsql.common.format.byterow;

import com.bilibili.bsql.common.format.converter.CustomRowConverter;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.TimeZone;

import static org.apache.flink.table.utils.SaberStringUtils.unicodeStringDecode;

/**
 * @author: zhuzhengjun
 * @date: 2021/7/5 8:16 下午
 */
public class ByteRowDeserialization extends AbstractDeserializationSchema<RowData> {
	private static final long serialVersionUID = 2385115520960444192L;

	private static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();
	private final String[] fieldNames;
	private final TypeInformation<?>[] fieldTypes;
	private final String delimiterKey;
	private final RowType rowType;
	private final CustomRowConverter converter;


	public ByteRowDeserialization(TypeInformation<RowData> typeInfo, RowType rowType,
								  String delimiterKey) {
		this.fieldNames = ((RowDataTypeInfo) typeInfo).getFieldNames();
		this.fieldTypes = ((RowDataTypeInfo) typeInfo).getFieldTypes();
		this.rowType = rowType;

		this.delimiterKey = unicodeStringDecode(delimiterKey);
		this.converter = new CustomRowConverter(rowType);
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		this.converter.setRowConverterMetricGroup(context.getMetricGroup());
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		GenericRowData row = new GenericRowData(1);
		row.setField(0, message);
		return row;
	}

}
