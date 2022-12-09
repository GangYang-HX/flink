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

package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.formats.protobuf.PbSchemaValidator;
import org.apache.flink.formats.protobuf.metrics.PbMetricsWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.QueryServiceMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from Protobuf to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a protobuf object and reads the specified
 * fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
public class PbRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PbRowDataDeserializationSchema.class);

    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;

    private final PbFormatConfig formatConfig;

    private transient ProtoToRowConverter protoToRowConverter;

    private RowData defaultRowData;

    private static final String PB_METRICS = "PbMetrics";

    private static final String DIRTY_DATA = "dirtyData";

    private static final String DEFAULT_VALUE = "defaultValue";

    private PbMetricsWrapper pbMetricsWrapper;

    public PbRowDataDeserializationSchema(
            RowType rowType, TypeInformation<RowData> resultTypeInfo, PbFormatConfig formatConfig) {
        checkNotNull(rowType, "rowType cannot be null");
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
        this.formatConfig = formatConfig;
        // do it in client side to report error in the first place
        new PbSchemaValidator(
                        PbFormatUtils.getDescriptor(formatConfig.getMessageClassName()), rowType)
                .validate();
        // this step is only used to validate codegen in client side in the first place
        try {
            // validate converter in client side to early detect errors
            protoToRowConverter = new ProtoToRowConverter(rowType, formatConfig);
        } catch (PbCodegenException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        protoToRowConverter = new ProtoToRowConverter(rowType, formatConfig);
        this.defaultRowData = protoToRowConverter.convertDefaultRow();
        MetricGroup metricGroup =
                context.getMetricGroup().addGroup(PB_METRICS, "", QueryServiceMode.DISABLED);
        pbMetricsWrapper =
                new PbMetricsWrapper()
                        .setDirtyData(metricGroup.counter(DIRTY_DATA))
                        .setDefaultValue(metricGroup.counter(DEFAULT_VALUE));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            RowData rowData = protoToRowConverter.convertProtoBinaryToRow(message);
            return rowData;
        } catch (Throwable t) {
            if (formatConfig.isIgnoreParseErrors()) {
                if (formatConfig.isAddDefaultValue()) {
                    pbMetricsWrapper.defaultValue();
                    return defaultRowData;
                }
                pbMetricsWrapper.dirtyData();
                return null;
            }
            throw new IOException("Failed to deserialize PB object.", t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PbRowDataDeserializationSchema that = (PbRowDataDeserializationSchema) o;
        return Objects.equals(rowType, that.rowType)
                && Objects.equals(resultTypeInfo, that.resultTypeInfo)
                && Objects.equals(formatConfig, that.formatConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, resultTypeInfo, formatConfig);
    }
}
