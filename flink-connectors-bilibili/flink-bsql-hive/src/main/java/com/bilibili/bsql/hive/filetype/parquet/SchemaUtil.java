package com.bilibili.bsql.hive.filetype.parquet;

import org.apache.avro.Schema;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by haozhugogo on 2020/5/6.
 */
public class SchemaUtil {

    public static Schema convertSchema(String[] fieldNames, Class<?>[] fieldClasses) {
        List<Schema.Field> fieldList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Tuple2<Schema, Object> tuple2 = SchemaUtil.classConvertSchema(fieldClasses[i]);
            Schema.Field field = new Schema.Field(fieldNames[i], tuple2.f0, fieldNames[i], tuple2.f1);
            fieldList.add(field);
        }
        Schema schema = Schema.createRecord("name", "doc", "namespace", true, fieldList);
        return schema;
    }

	public static Class<?> classConvertSchemaName(Class<?> javaClass) {
		String javaClassName = javaClass.getSimpleName();
		switch (javaClassName) {
			case "VarCharType":
				return String.class;
			case "BigIntType":
				return Long.class;
			case "IntType":
				return Integer.class;
			case "BooleanType":
				return Boolean.class;
			case "SmallIntType":
				return Integer.class;
			case "FloatType":
				return Float.class;
			case "DoubleType":
				return Double.class;
			case "BytesType":
			case "VarBinaryType":
				return byte[].class;

			default:
				return String.class;
		}
	}


	public static Tuple2<Schema, Object> classConvertSchema(Class<?> javaClass) {
        String javaClassName = javaClass.getSimpleName();
        Schema schema = null;
        switch (javaClassName) {
            case "BooleanType":
                schema = Schema.create(Schema.Type.BOOLEAN);
                return new Tuple2(schema, null);
            case "IntType":
                schema = Schema.create(Schema.Type.INT);
                return new Tuple2(schema, null);
            case "BigIntType":
                schema = Schema.create(Schema.Type.LONG);
                return new Tuple2(schema, null);
            // case "Byte":
            // schema = Schema.create(Schema.Type.BYTES);
            // return new Tuple2(schema, null);
            case "SmallIntType":
                schema = Schema.create(Schema.Type.INT);
                return new Tuple2(schema, null);
            case "VarCharType":
                schema = Schema.create(Schema.Type.STRING);
                return new Tuple2(schema, null);
            case "FloatType":
                schema = Schema.create(Schema.Type.FLOAT);
                return new Tuple2(schema, null);
            case "DoubleType":
                schema = Schema.create(Schema.Type.DOUBLE);
                return new Tuple2(schema, null);
            // case "Date":
            // schema = Schema.create(Schema.Type.STRING);
            // return new Tuple2(schema, "");
            // case "Timestamp":
            // schema = Schema.create(Schema.Type.STRING);
            // return new Tuple2(schema, "");
            // case "BigDecimal":
            // schema = Schema.create(Schema.Type.LONG);
            // return new Tuple2(schema, -999);
            case "BytesType":
                schema = Schema.create(Schema.Type.BYTES);
                return new Tuple2(schema, -999);
            default:
                schema = Schema.create(Schema.Type.STRING);
                return new Tuple2(schema, "");
        }
    }
}
