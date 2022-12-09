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

package com.bilibili.bsql.hive.filetype.compress;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import com.bilibili.bsql.hive.filetype.compress.writer.HadoopCompressionBulkWriter;
import com.bilibili.bsql.hive.filetype.compress.writer.extractor.Extractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory that creates for {@link BulkWriter bulk writers} that, when provided
 * with a {@link CompressionCodec}, they compress the data they write. If no codec is
 * provided, the data is written in bulk but uncompressed.
 *
 * @param <IN> The type of element to write.
 */
@PublicEvolving
public class CompressWriterFactory<IN> implements BulkWriter.Factory<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(CompressWriterFactory.class);

    private final Extractor<IN> extractor;
	private final String rowDelim;
	private final Map<String, String> hadoopConfigMap;

    private transient CompressionCodec hadoopCodec;
    private String hadoopCodecName;
    private String codecExtension;


    /**
     * Creates a new CompressWriterFactory using the given {@link Extractor} to assemble
     * based on whether a Hadoop CompressionCodec name is specified.
     *
     * @param extractor Extractor to extract the element
     */
    public CompressWriterFactory(Extractor<IN> extractor, String rowDelim) {
        this.extractor = checkNotNull(extractor, "Extractor cannot be null");
        this.rowDelim = checkNotNull(rowDelim,"rowDelim cannot be null");
        this.hadoopConfigMap = new HashMap<>();
    }


    public CompressWriterFactory build(String codecName, Configuration conf) {

        this.hadoopCodecName = checkNotNull(codecName, "codecName cannot be null");

        CompressionCodec codec = new CompressionCodecFactory(conf).getCodecByName(hadoopCodecName);
        this.codecExtension = null == codec ? null : codec.getDefaultExtension();

        for (Map.Entry<String, String> entry : conf) {
            hadoopConfigMap.put(entry.getKey(), entry.getValue());
        }

        return this;
    }

    @Override
    public BulkWriter<IN> create(FSDataOutputStream out) throws IOException {

        initializeCompressionCodecIfNecessary();

        return new HadoopCompressionBulkWriter<>(extractor, rowDelim, hadoopCodec.createOutputStream(out));
    }

    public String getExtension() {
        return (hadoopCodecName != null) ? this.codecExtension : "";
    }

    private void initializeCompressionCodecIfNecessary() {
        if (hadoopCodec == null) {
            Configuration conf = new Configuration();

            for (Map.Entry<String, String> entry : hadoopConfigMap.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }

            hadoopCodec = new CompressionCodecFactory(conf).getCodecByName(this.hadoopCodecName);
			LOG.info("hadoopCodecName:{},hadoopCodec:{}",hadoopCodecName,hadoopCodec);
        }
    }
}
