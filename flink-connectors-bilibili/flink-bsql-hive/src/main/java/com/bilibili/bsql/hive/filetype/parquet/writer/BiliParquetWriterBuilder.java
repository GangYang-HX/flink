package com.bilibili.bsql.hive.filetype.parquet.writer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

/**
 * Created by haozhugogo on 2020/5/12.
 */
public class BiliParquetWriterBuilder extends ParquetWriter.Builder<Row, BiliParquetWriterBuilder> {

    private Schema schema = null;
    private GenericData model = SpecificData.get();

    public BiliParquetWriterBuilder(Path file) {
        super(file);
    }

    public BiliParquetWriterBuilder(OutputFile file) {
        super(file);
    }

    public BiliParquetWriterBuilder withSchema(Schema schema) {
        this.schema = schema;
        return this;
    }

    public BiliParquetWriterBuilder withDataModel(GenericData model) {
        this.model = model;
        return this;
    }

    @Override
    protected BiliParquetWriterBuilder self() {
        return this;
    }

    @Override
    protected BiliAvroWriteSupport getWriteSupport(Configuration conf) {
        return new BiliAvroWriteSupport(new AvroSchemaConverter(conf).convert(schema), schema, model);
    }
}
