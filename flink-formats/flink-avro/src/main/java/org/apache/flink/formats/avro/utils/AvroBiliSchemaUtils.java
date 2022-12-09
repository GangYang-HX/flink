package org.apache.flink.formats.avro.utils;

import org.apache.flink.util.StringUtils;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.stream.Collectors;

/** Utilities for bili-canal. */
public class AvroBiliSchemaUtils {
    private static ThreadLocal<BinaryDecoder> reuseDecoder =
            ThreadLocal.withInitial(
                    () -> {
                        return null;
                    });

    public static GenericRecord bytesToAvro(byte[] bytes, Schema writerSchema, Schema readerSchema)
            throws IOException {
        BinaryDecoder decoder =
                DecoderFactory.get().binaryDecoder(bytes, (BinaryDecoder) reuseDecoder.get());
        reuseDecoder.set(decoder);
        GenericDatumReader<GenericRecord> reader =
                new GenericDatumReader(writerSchema, readerSchema);
        return (GenericRecord) reader.read(null, decoder);
    }

    public static String getNestedFieldValAsString(
            GenericRecord record, String fieldName, boolean returnNullIfNotFound) {
        Object obj = getNestedFieldVal(record, fieldName, returnNullIfNotFound);
        return StringUtils.objToString(obj);
    }

    public static Object getNestedFieldVal(
            GenericRecord record, String fieldName, boolean returnNullIfNotFound) {
        String[] parts = fieldName.split("\\.");
        GenericRecord valueNode = record;

        int i;
        for (i = 0; i < parts.length; ++i) {
            String part = parts[i];
            Object val = valueNode.get(part);
            if (val == null) {
                break;
            }

            if (i == parts.length - 1) {
                Schema fieldSchema = valueNode.getSchema().getField(part).schema();
                return convertValueForSpecificDataTypes(fieldSchema, val);
            }

            if (!(val instanceof GenericRecord)) {
                throw new RuntimeException("Cannot find a record at part value :" + part);
            }

            valueNode = (GenericRecord) val;
        }

        if (returnNullIfNotFound) {
            return null;
        } else if (valueNode.getSchema().getField(parts[i]) == null) {
            throw new RuntimeException(
                    fieldName
                            + "(Part -"
                            + parts[i]
                            + ") field not found in record. Acceptable fields were :"
                            + valueNode.getSchema().getFields().stream()
                                    .map(Schema.Field::name)
                                    .collect(Collectors.toList()));
        } else {
            throw new RuntimeException("The value of " + parts[i] + " can not be null");
        }
    }

    public static Object convertValueForSpecificDataTypes(Schema fieldSchema, Object fieldValue) {
        if (fieldSchema == null) {
            return fieldValue;
        } else {
            if (fieldSchema.getType() == Schema.Type.UNION) {
                Iterator var2 = fieldSchema.getTypes().iterator();

                while (var2.hasNext()) {
                    Schema schema = (Schema) var2.next();
                    if (schema.getType() != Schema.Type.NULL) {
                        return convertValueForAvroLogicalTypes(schema, fieldValue);
                    }
                }
            }

            return convertValueForAvroLogicalTypes(fieldSchema, fieldValue);
        }
    }

    private static Object convertValueForAvroLogicalTypes(Schema fieldSchema, Object fieldValue) {
        if (fieldSchema.getLogicalType() == LogicalTypes.date()) {
            return LocalDate.ofEpochDay(Long.parseLong(fieldValue.toString()));
        } else {
            if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
                LogicalTypes.Decimal dc = (LogicalTypes.Decimal) fieldSchema.getLogicalType();
                Conversions.DecimalConversion decimalConversion =
                        new Conversions.DecimalConversion();
                if (fieldSchema.getType() == Schema.Type.FIXED) {
                    return decimalConversion.fromFixed(
                            (GenericFixed) fieldValue,
                            fieldSchema,
                            LogicalTypes.decimal(dc.getPrecision(), dc.getScale()));
                }

                if (fieldSchema.getType() == Schema.Type.BYTES) {
                    return decimalConversion.fromBytes(
                            (ByteBuffer) fieldValue,
                            fieldSchema,
                            LogicalTypes.decimal(dc.getPrecision(), dc.getScale()));
                }
            }

            return fieldValue;
        }
    }

    public static byte[] avroToJson(GenericRecord record, boolean pretty) throws Exception {
        DatumWriter<Object> writer = new GenericDatumWriter(record.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out, pretty);
        writer.write(record, jsonEncoder);
        jsonEncoder.flush();
        return out.toByteArray();
    }
}
