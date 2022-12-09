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

package com.bilibili.bsql.hive.filetype.compress.writer;

import org.apache.flink.api.common.serialization.BulkWriter;

import com.bilibili.bsql.hive.filetype.compress.writer.extractor.Extractor;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * A {@link BulkWriter} implementation that writes data that have been
 * compressed using Hadoop {@link org.apache.hadoop.io.compress.CompressionCodec}.
 *
 * @param <T> The type of element to write.
 */
public class HadoopCompressionBulkWriter<T> implements BulkWriter<T> {

	private final Extractor<T> extractor;
	private final OutputStream outputStream;
	private final String rowDelim;

	public HadoopCompressionBulkWriter(Extractor<T> extractor, String rowDelim, OutputStream outputStream) {
		this.extractor = extractor;
		this.outputStream = outputStream;
		this.rowDelim = rowDelim;
	}

	@Override
	public void addElement(T element) throws IOException {
		outputStream.write(extractor.extract(element));
		outputStream.write(rowDelim.getBytes(Charset.forName("UTF-8")));

	}

	@Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void finish() throws IOException {
        ((CompressionOutputStream) outputStream).finish();
    }
}
