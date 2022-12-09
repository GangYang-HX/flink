/**
 * Bilibili.com Inc. Copyright (c) 2009-2019 All Rights Reserved.
 */
package com.bilibili.bsql.common.format;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.utils.SaberStringUtils.unicodeStringDecode;

/**
 * CustomDelimiterSerialization
 * 
 * @author zhouxiaogang
 */
public class CustomDelimiterSerialization implements SerializationSchema<RowData> {

    private static final long    serialVersionUID = -2885556750743978939L;

    public TypeInformation<RowData> typeInfo;

    public final String         delimiterKey;

    public CustomDelimiterSerialization(TypeInformation<RowData> typeInfo, String delimiterKey) {
        Preconditions.checkNotNull(typeInfo, "Type information");
        this.typeInfo = typeInfo;
        this.delimiterKey = unicodeStringDecode(delimiterKey);

    }

    @Override
    public byte[] serialize(RowData row) {

    	/**
		 * no thing done here
		 *
		 * */
		throw new UnsupportedOperationException();
    }
}
