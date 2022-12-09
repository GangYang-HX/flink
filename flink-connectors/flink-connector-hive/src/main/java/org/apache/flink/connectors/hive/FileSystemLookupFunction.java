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

package org.apache.flink.connectors.hive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.file.table.PartitionFetcher;
import org.apache.flink.connector.file.table.PartitionReader;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.QueryServiceMode;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Lookup function for filesystem connector tables.
 *
 * <p>The hive connector and filesystem connector share read/write files code. Currently, this
 * function only used in hive connector.
 */
public class FileSystemLookupFunction<P> extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemLookupFunction.class);

    // the max number of retries before throwing exception, in case of failure to load the table
    // into cache
    private static final int MAX_RETRIES = 3;
    // interval between retries
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

    private final PartitionFetcher<P> partitionFetcher;
    private final PartitionFetcher.Context<P> fetcherContext;
    private final PartitionReader<P, RowData> partitionReader;
    private final RowData.FieldGetter[] lookupFieldGetters;
    private final Duration reloadInterval;
    private final TypeSerializer<RowData> serializer;
    private final RowType rowType;
    private Histogram threadRtQuery;

    // cache for lookup data
    public static transient Cache<RowData, List<RowData>> cache;

    public FileSystemLookupFunction(
            PartitionFetcher<P> partitionFetcher,
            PartitionFetcher.Context<P> fetcherContext,
            PartitionReader<P, RowData> partitionReader,
            RowType rowType,
            int[] lookupKeys,
            Duration reloadInterval) {
        this.fetcherContext = fetcherContext;
        this.partitionFetcher = partitionFetcher;
        this.partitionReader = partitionReader;
        this.rowType = rowType;
        this.lookupFieldGetters = new RowData.FieldGetter[lookupKeys.length];
        for (int i = 0; i < lookupKeys.length; i++) {
            lookupFieldGetters[i] =
                    RowData.createFieldGetter(rowType.getTypeAt(lookupKeys[i]), lookupKeys[i]);
        }
        this.reloadInterval = reloadInterval;
        this.serializer = InternalSerializers.create(rowType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        cache = CacheBuilder.newBuilder().build();
        fetcherContext.open();
        HiveLookupCacheFactory.getInstance().initCache(() -> checkCacheReload(), reloadInterval);

        // add metric
        this.threadRtQuery =
                context.getMetricGroup()
                        .addGroup("thread_hive", QueryServiceMode.DISABLED)
                        .histogram("rt", new DescriptiveStatisticsHistogram(10_000));
    }

    @Override
    public TypeInformation<RowData> getResultType() {
        return InternalTypeInfo.of(rowType);
    }

    public void eval(Object... values) {
        RowData lookupKey = GenericRowData.of(values);
        long start = System.nanoTime();
        List<RowData> matchedRows = cache.getIfPresent(lookupKey);
        if (matchedRows != null) {
            setQueryRtMetric(start);
            for (RowData matchedRow : matchedRows) {
                collect(matchedRow);
            }
        }
    }

    private void setQueryRtMetric(long start) {
        this.threadRtQuery.update(System.nanoTime() - start);
    }

    /**
     * Do not clear the cache before updating, keep the same logic as Flink-1.11, and continuously
     * update the key.
     */
    private void checkCacheReload() {
        LOG.info("load lookup data to cache");
        int numRetry = 0;
        while (true) {
            try {
                long count = 0;
                Cache<RowData, List<RowData>> currentCache = CacheBuilder.newBuilder().build();
                GenericRowData reuse = new GenericRowData(rowType.getFieldCount());
                partitionReader.open(partitionFetcher.fetch(fetcherContext));
                RowData row;
                while ((row = partitionReader.read(reuse)) != null) {
                    count++;
                    RowData rowData = serializer.copy(row);
                    RowData key = extractLookupKey(rowData);
                    List<RowData> rows = currentCache.get(key, () -> new ArrayList<>());
                    rows.add(rowData);
                }
                cache = currentCache;
                partitionReader.close();
                LOG.info("Loaded {} row(s) into lookup join cache", count);
                return;
            } catch (Exception e) {
                if (numRetry >= MAX_RETRIES) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Failed to load table into cache after %d retries", numRetry),
                            e);
                }
                numRetry++;
                long toSleep = numRetry * RETRY_INTERVAL.toMillis();
                LOG.warn(
                        String.format(
                                "Failed to load table into cache, will retry in %d seconds",
                                toSleep / 1000),
                        e);
                try {
                    Thread.sleep(toSleep);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while waiting to retry failed cache load, aborting");
                    throw new FlinkRuntimeException(ex);
                }
            }
        }
    }

    private RowData extractLookupKey(RowData row) {
        GenericRowData key = new GenericRowData(lookupFieldGetters.length);
        for (int i = 0; i < lookupFieldGetters.length; i++) {
            key.setField(i, lookupFieldGetters[i].getFieldOrNull(row));
        }
        return key;
    }

    @Override
    public void close() throws Exception {
        this.fetcherContext.close();
    }

    @VisibleForTesting
    public Duration getReloadInterval() {
        return reloadInterval;
    }

    @VisibleForTesting
    public PartitionFetcher<P> getPartitionFetcher() {
        return partitionFetcher;
    }

    @VisibleForTesting
    public PartitionFetcher.Context<P> getFetcherContext() {
        return fetcherContext;
    }

    @VisibleForTesting
    public PartitionReader<P, RowData> getPartitionReader() {
        return partitionReader;
    }
}
