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

package com.bilibili.bsql.hive.filetype.orc.writer;

import com.bilibili.bsql.hive.filetype.orc.vector.Vectorizer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Writer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link BulkWriter} implementation that writes data in ORC format.
 *
 * @param <T> The type of element written.
 */
@Internal
public class OrcBulkWriter<T> implements BulkWriter<T> {

    private final Writer writer;
    private final Vectorizer<T> vectorizer;
    private final VectorizedRowBatch rowBatch;
    private final int batchSize;


	OrcBulkWriter(Vectorizer<T> vectorizer, Writer writer, int bufferSize, int batchSize) {
		this.vectorizer = checkNotNull(vectorizer);
		this.writer = checkNotNull(writer);
		this.rowBatch = vectorizer.getSchema().createRowBatch(bufferSize);

		// Configure the vectorizer with the writer so that users can add
		// metadata on the fly through the Vectorizer#vectorize(...) method.
		this.vectorizer.setWriter(this.writer);
		this.batchSize = batchSize;
	}

    @Override
    public void addElement(T element) throws IOException {
        vectorizer.vectorize(element, rowBatch);
        if (rowBatch.size == batchSize) {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        }
    }

    @Override
    public void flush() throws IOException {
        if (rowBatch.size != 0) {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        }
    }

    @Override
    public void finish() throws IOException {
        flush();
        writer.close();
    }

}
