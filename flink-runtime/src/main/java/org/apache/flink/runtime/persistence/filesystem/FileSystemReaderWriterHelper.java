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

package org.apache.flink.runtime.persistence.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Helper class to read and write to FileSystem. */
public class FileSystemReaderWriterHelper<T extends Serializable>
        implements FileSystemReaderWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemReaderWriterHelper.class);

    private final FileSystem fs;

    private final int maxRetry;

    private final int retryInterval;

    public FileSystemReaderWriterHelper(FileSystem fs, Configuration configuration) {
        this.fs = checkNotNull(fs, "FileSystem");
        this.maxRetry =
                configuration.getInteger(HighAvailabilityOptions.HA_FILESYSTEM_MAX_RETRY_ATTEMPTS);
        this.retryInterval =
                configuration.getInteger(HighAvailabilityOptions.HA_FILESYSTEM_RETRY_WAIT);
    }

    @Override
    public T read(Path path) throws IOException {
        if (!fs.exists(path)) {
            throw new FileNotFoundException(path.getPath());
        }

        int retries = 0;
        while (retries < maxRetry) {
            try (FSDataInputStream inputStream = fs.open(path)) {
                return InstantiationUtil.deserializeObject(
                        inputStream, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new FlinkRuntimeException(e);
            } catch (IOException e) {
                retries += 1;

                if (retries < maxRetry) {
                    try {
                        Thread.sleep(retryInterval);
                    } catch (InterruptedException ie) {
                        LOG.warn("Read data from file system interrupted.");
                    }
                } else {
                    throw e;
                }
            }
        }

        // never come here, just avoid compile error
        throw new IOException("Failed to read data from file system");
    }

    @Override
    public void write(Path path, T t) throws IOException {
        int retries = 0;
        while (retries < maxRetry) {
            try (FSDataOutputStream outStream = fs.create(path, FileSystem.WriteMode.OVERWRITE)) {
                InstantiationUtil.serializeObject(outStream, t);
                return;
            } catch (IOException e) {
                retries += 1;

                if (retries < maxRetry) {
                    try {
                        Thread.sleep(retryInterval);
                    } catch (InterruptedException ie) {
                        LOG.warn("Read data from file system interrupted.");
                    }
                } else {
                    throw e;
                }
            }
        }
    }
}
