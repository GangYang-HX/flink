package org.apache.flink.formats.json.bilicanal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.utils.AvroBiliSchemaUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.bilicanal.BiliCanalJsonDecodingFormat.ReadableMetadata;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

/** Deserialization schema from bili-Canal JSON to Flink Table/SQL internal data structure. */
public class BiliCanalJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final String OP_INSERT = "insert";
    private static final String OP_UPDATE = "update";
    private static final String OP_DELETE = "delete";
    private static final String OP_CREATE = "create";

    private static final String packagingSchemaString =
            "{\"type\":\"record\",\"name\":\"AvroFlumeEvent\","
                    + "\"namespace\":\"org.apache.flume.source.avro\","
                    + "\"fields\":[{\"name\":\"headers\","
                    + "\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
                    + "{\"name\":\"body\",\"type\":\"string\"}]}";
    private Schema schema;
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /** Flag that indicates that an additional projection is required for metadata. */
    private final boolean hasMetadata;

    /** Metadata to be extracted for every record. */
    private final MetadataConverter[] metadataConverters;

    /** {@link TypeInformation} of the produced {@link RowData} (physical + meta data). */
    private final TypeInformation<RowData> producedTypeInfo;

    /** Only read changelogs from the specific database. */
    private final @Nullable String database;

    /** Only read changelogs from the specific table. */
    private final @Nullable String table;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Number of fields. */
    private final int fieldCount;

    /** Pattern of the specific database. */
    private BiliCanalJsonDeserializationSchema(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo,
            @Nullable String database,
            @Nullable String table,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        final RowType jsonRowType = createJsonRowType(physicalDataType, requestedMetadata);
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
                        // the result type is never used, so it's fine to pass in the produced type
                        // info
                        producedTypeInfo,
                        false, // ignoreParseErrors already contains the functionality of
                        // failOnMissingField
                        ignoreParseErrors,
                        timestampFormat);
        this.hasMetadata = requestedMetadata.size() > 0;
        this.metadataConverters = createMetadataConverters(jsonRowType, requestedMetadata);
        this.producedTypeInfo = producedTypeInfo;
        this.database = database;
        this.table = table;
        this.ignoreParseErrors = ignoreParseErrors;
        final RowType physicalRowType = ((RowType) physicalDataType.getLogicalType());
        this.fieldCount = physicalRowType.getFieldCount();
    }

    public static Builder builder(
            DataType physicalDataType,
            List<ReadableMetadata> requestedMetadata,
            TypeInformation<RowData> producedTypeInfo) {
        return new BiliCanalJsonDeserializationSchema.Builder(
                physicalDataType, requestedMetadata, producedTypeInfo);
    }

    /** A builder for creating a {@link BiliCanalJsonDeserializationSchema}. */
    @Internal
    public static final class Builder {
        private final DataType physicalDataType;
        private final List<ReadableMetadata> requestedMetadata;
        private final TypeInformation<RowData> producedTypeInfo;
        private String database = null;
        private String table = null;
        private boolean ignoreParseErrors = false;
        private TimestampFormat timestampFormat = TimestampFormat.SQL;

        private Builder(
                DataType physicalDataType,
                List<ReadableMetadata> requestedMetadata,
                TypeInformation<RowData> producedTypeInfo) {
            this.physicalDataType = physicalDataType;
            this.requestedMetadata = requestedMetadata;
            this.producedTypeInfo = producedTypeInfo;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public Builder setTimestampFormat(TimestampFormat timestampFormat) {
            this.timestampFormat = timestampFormat;
            return this;
        }

        public BiliCanalJsonDeserializationSchema build() {
            return new BiliCanalJsonDeserializationSchema(
                    physicalDataType,
                    requestedMetadata,
                    producedTypeInfo,
                    database,
                    table,
                    ignoreParseErrors,
                    timestampFormat);
        }
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonDeserializer.open(context);
        this.schema = new Schema.Parser().parse(packagingSchemaString);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(@Nullable byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        try {
            byte[] data = convert2bytes(message);
            final JsonNode root = jsonDeserializer.deserializeToJsonNode(data);
            final GenericRowData row = (GenericRowData) jsonDeserializer.convertToRowData(root);
            String type = root.get("action").asText();
            if (OP_INSERT.equals(type)) {
                // "data" field is an array of row, contains inserted rows
                GenericRowData insert = (GenericRowData) row.getRow(5, fieldCount);
                insert.setRowKind(RowKind.INSERT);
                emitRow(row, insert, out);
            } else if (OP_UPDATE.equals(type)) {
                // the underlying JSON deserialization schema always produce GenericRowData.
                GenericRowData before = (GenericRowData) row.getRow(4, fieldCount);
                GenericRowData after = (GenericRowData) row.getRow(5, fieldCount);
                before.setRowKind(RowKind.UPDATE_BEFORE);
                after.setRowKind(RowKind.UPDATE_AFTER);
                emitRow(row, before, out);
                emitRow(row, after, out);
            } else if (OP_DELETE.equals(type)) {
                // "data" field is an array of row, contains deleted rows
                GenericRowData insert = (GenericRowData) row.getRow(5, fieldCount);
                insert.setRowKind(RowKind.DELETE);
                emitRow(row, insert, out);
            } else if (OP_CREATE.equals(type)) {
                // "data" field is null and "type" is "CREATE" which means
                // this is a DDL change event, and we should skip it.
                return;
            } else {
                if (!ignoreParseErrors) {
                    throw new IOException(
                            format(
                                    "Unknown \"type\" value \"%s\". The bili-canal JSON message is '%s'",
                                    type, new String(message)));
                }
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Corrupt bili-canal JSON message '%s'.", new String(message)), t);
            }
        }
    }

    private byte[] convert2bytes(byte[] bytes) throws IOException {
        byte[] data = new byte[bytes.length - 4];
        System.arraycopy(bytes, 4, data, 0, data.length);
        String body =
                AvroBiliSchemaUtils.getNestedFieldValAsString(
                        AvroBiliSchemaUtils.bytesToAvro(data, schema, schema), "body", true);
        return body.getBytes();
    }

    private void emitRow(
            GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> out) {
        // shortcut in case no output projection is required
        if (!hasMetadata) {
            out.collect(physicalRow);
            return;
        }
        final int physicalArity = physicalRow.getArity();
        final int metadataArity = metadataConverters.length;
        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);
        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
            producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
        }
        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    physicalArity + metadataPos, metadataConverters[metadataPos].convert(rootRow));
        }
        out.collect(producedRow);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BiliCanalJsonDeserializationSchema that = (BiliCanalJsonDeserializationSchema) o;
        return Objects.equals(jsonDeserializer, that.jsonDeserializer)
                && hasMetadata == that.hasMetadata
                && Objects.equals(producedTypeInfo, that.producedTypeInfo)
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && ignoreParseErrors == that.ignoreParseErrors
                && fieldCount == that.fieldCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jsonDeserializer,
                hasMetadata,
                producedTypeInfo,
                database,
                table,
                ignoreParseErrors,
                fieldCount);
    }

    private static RowType createJsonRowType(
            DataType physicalDataType, List<ReadableMetadata> readableMetadata) {
        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("action", DataTypes.STRING()),
                        DataTypes.FIELD("schema", DataTypes.STRING()),
                        DataTypes.FIELD("table", DataTypes.STRING()),
                        DataTypes.FIELD("pk_names", DataTypes.STRING()),
                        DataTypes.FIELD("old", physicalDataType),
                        DataTypes.FIELD("new", physicalDataType));
        // append fields that are required for reading metadata in the root
        final List<DataTypes.Field> rootMetadataFields =
                readableMetadata.stream()
                        .map(m -> m.requiredJsonField)
                        .distinct()
                        .collect(Collectors.toList());
        return (RowType) DataTypeUtils.appendRowFields(root, rootMetadataFields).getLogicalType();
    }

    private static MetadataConverter[] createMetadataConverters(
            RowType jsonRowType, List<ReadableMetadata> requestedMetadata) {
        return requestedMetadata.stream()
                .map(m -> convert(jsonRowType, m))
                .toArray(MetadataConverter[]::new);
    }

    private static MetadataConverter convert(RowType jsonRowType, ReadableMetadata metadata) {
        final int pos = jsonRowType.getFieldNames().indexOf(metadata.requiredJsonField.getName());
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(GenericRowData root, int unused) {
                return metadata.converter.convert(root, pos);
            }
        };
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Converter that extracts a metadata field from the row that comes out of the JSON schema and
     * converts it to the desired data type.
     */
    interface MetadataConverter extends Serializable {

        // Method for top-level access.
        default Object convert(GenericRowData row) {
            return convert(row, -1);
        }

        Object convert(GenericRowData row, int pos);
    }
}
