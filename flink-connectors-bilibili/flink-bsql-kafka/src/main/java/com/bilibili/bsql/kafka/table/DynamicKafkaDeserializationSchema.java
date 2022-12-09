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

package com.bilibili.bsql.kafka.table;

import com.bilibili.bsql.kafka.ability.metadata.read.ReadableMetadata;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

/**
 * A specific {@link KafkaSerializationSchema} for {@link com.bilibili.bsql.kafka.table.BsqlKafka010DynamicSource}.
 */
public class DynamicKafkaDeserializationSchema implements KafkaDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final OutputProjectionCollector outputCollector;

    public DynamicKafkaDeserializationSchema(
            DeserializationSchema<RowData> valueDeserialization,
            int[] physicalColumnIndices,
            int[] metadataColumnIndices,
            int[] selectedColumns,
            ReadableMetadata.MetadataConverter[] metadataConverters) {
        this.valueDeserialization = valueDeserialization;
        this.outputCollector =
                new OutputProjectionCollector(
                        physicalColumnIndices,
                        metadataColumnIndices,
                        selectedColumns,
                        metadataConverters);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        valueDeserialization.open(context);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public RowData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        throw new IllegalStateException("A collector is required for deserializing.");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector)
            throws Exception {
        outputCollector.inputRecord = record;
        outputCollector.outputCollector = collector;
        valueDeserialization.deserialize(record.value(), outputCollector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return valueDeserialization.getProducedType();
    }

    // --------------------------------------------------------------------------------------------

    // --------------------------------------------------------------------------------------------

    /**
     * Emits a row with key, value, and metadata fields.
     *
     * <p>The collector is able to handle the following kinds of keys:
     *
     * <ul>
     *   <li>No key is used.
     *   <li>A key is used.
     *   <li>The deserialization schema emits multiple keys.
     *   <li>Keys and values have overlapping fields.
     *   <li>Keys are used and value is null.
     * </ul>
     */
    private static final class OutputProjectionCollector
            implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int[] physicalColumnIndices;

        private final int[] metadataColumnIndices;

        private final int[] selectedColumns;

        private final ReadableMetadata.MetadataConverter[] metadataConverters;

        private transient ConsumerRecord<?, ?> inputRecord;

        private transient Collector<RowData> outputCollector;

        OutputProjectionCollector(
                int[] physicalColumnIndices,
                int[] metadataColumnIndices,
                int[] selectedColumns,
                ReadableMetadata.MetadataConverter[] metadataConverters) {
            this.physicalColumnIndices = physicalColumnIndices;
            this.metadataColumnIndices = metadataColumnIndices;
            this.selectedColumns = selectedColumns;
            this.metadataConverters = metadataConverters;
        }

        @Override
        public void collect(RowData row) {
            GenericRowData physicalValueRow = (GenericRowData)row;

            final int metadataArity = metadataColumnIndices.length;
            final int physicalArity = physicalColumnIndices.length;
            final GenericRowData tmpRow = new GenericRowData(metadataArity + physicalArity);
            for (int valPos = 0; valPos < physicalArity; valPos++) {
                tmpRow.setField(physicalColumnIndices[valPos], physicalValueRow.getField(valPos));
            }
            for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
                tmpRow.setField(
                        metadataColumnIndices[metadataPos],
                        metadataConverters[metadataPos].read(inputRecord));
            }
            final GenericRowData producedRow = new GenericRowData(selectedColumns.length);
            for(int i=0;i<selectedColumns.length;i++){
                producedRow.setField(i,tmpRow.getField(selectedColumns[i]));
            }

            outputCollector.collect(producedRow);
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}
