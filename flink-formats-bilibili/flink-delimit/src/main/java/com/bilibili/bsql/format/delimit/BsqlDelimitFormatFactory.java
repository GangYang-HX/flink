/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bilibili.bsql.format.delimit;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.format.delimit.byterow.ByteRowDeserialization;
import com.bilibili.bsql.format.delimit.byterow.LancerByteRowDeserialization;
import com.bilibili.bsql.format.delimit.raw.CustomDelimiterRawDeserialization;
import com.bilibili.bsql.format.delimit.raw.LancerCustomDelimiterRawDeserialization;

import java.util.HashSet;
import java.util.Set;

import static com.bilibili.bsql.format.delimit.BsqlDelimitConfig.DELIMITER_KEY;
import static com.bilibili.bsql.format.delimit.BsqlDelimitConfig.FORMAT;
import static com.bilibili.bsql.format.delimit.BsqlDelimitConfig.LANCER_SINK_DEST;
import static com.bilibili.bsql.format.delimit.BsqlDelimitConfig.NO_DEFAULT_VALUE;
import static com.bilibili.bsql.format.delimit.BsqlDelimitConfig.RAW_FORMAT;
import static com.bilibili.bsql.format.delimit.BsqlDelimitConfig.SINK_TRACE_ID;
import static com.bilibili.bsql.format.delimit.BsqlDelimitConfig.USE_LANCER_DEBUG;
import static com.bilibili.bsql.format.delimit.BsqlDelimitConfig.USE_LANCER_FORMAT;
import static com.bilibili.bsql.format.delimit.utils.DelimitStringUtils.unicodeStringDecode;

/**
 * Format factory for providing configured instances of delimit to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
public final class BsqlDelimitFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "bsql-delimit";
    public static final String FORMAT_BYTES = "bytes";

    @SuppressWarnings("unchecked")
    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);
        String delimitString = unicodeStringDecode(formatOptions.get(DELIMITER_KEY));
        Boolean useRawFormat = formatOptions.get(RAW_FORMAT);
        String format = formatOptions.get(FORMAT);
        Boolean noDefaultValue = formatOptions.get(NO_DEFAULT_VALUE);
        Boolean useLancerFormat = formatOptions.get(USE_LANCER_FORMAT);
        Boolean useLancerDebug = formatOptions.get(USE_LANCER_DEBUG);
        String lancerSinkDest = formatOptions.get(LANCER_SINK_DEST);
        String traceId = formatOptions.get(SINK_TRACE_ID);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {

                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);

                if (useLancerFormat) {
                    if (FORMAT_BYTES.equals(format)) {
                        return new LancerByteRowDeserialization(
                                rowDataTypeInfo, rowType, delimitString, lancerSinkDest, traceId);
                    } else {
                        return new LancerCustomDelimiterRawDeserialization(
                                rowDataTypeInfo,
                                rowType,
                                delimitString,
                                lancerSinkDest,
                                traceId,
                                useLancerDebug);
                    }
                }

                if (FORMAT_BYTES.equals(format)) {
                    return new ByteRowDeserialization(rowDataTypeInfo, rowType, delimitString);
                }

                return useRawFormat
                        ? new CustomDelimiterRawDeserialization(
                                rowDataTypeInfo, rowType, delimitString)
                        : new CustomOtherDelimiterDeserialization(
                                rowDataTypeInfo, rowType, delimitString, noDefaultValue);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);
        String delimitString = unicodeStringDecode(formatOptions.get(DELIMITER_KEY));

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(consumedDataType);

                return new CustomDelimiterSerialization(rowDataTypeInfo, delimitString);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DELIMITER_KEY);
        options.add(RAW_FORMAT);
        options.add(FORMAT);
        options.add(NO_DEFAULT_VALUE);
        options.add(USE_LANCER_FORMAT);
        options.add(USE_LANCER_DEBUG);
        options.add(LANCER_SINK_DEST);
        options.add(SINK_TRACE_ID);
        return options;
    }

    // ------------------------------------------------------------------------
    //  Validation
    // ------------------------------------------------------------------------

    static void validateFormatOptions(ReadableConfig tableOptions) {}

    /** Validates the option {@code option} value must be a Character. */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option) {
        if (tableOptions.getOptional(option).isPresent()) {
            if (tableOptions.get(option).length() != 1) {
                throw new ValidationException(
                        String.format(
                                "Option '%s.%s' must be a string with single character, but was: %s",
                                IDENTIFIER, option.key(), tableOptions.get(option)));
            }
        }
    }
}
