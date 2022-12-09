package com.bilibili.bsql.hive.filetype.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Created by haozhugogo on 2020/5/12.
 */
public class GenericRecordConverter {

    private List<Schema.Field> fields;
    private Schema schema;

    public GenericRecordConverter(Schema schema) {
        this.fields = schema.getFields();
        this.schema = schema;
    }

    public GenericRecord convert(Row row) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        for (int i = 0; i < row.getArity(); i++) {
            genericRecord.put(fields.get(i).name(), row.getField(i));
        }
        return genericRecord;
    }
}
