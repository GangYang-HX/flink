/** Bilibili.com Inc. Copyright (c) 2009-2020 All Rights Reserved. */
package com.bilibili.bsql.redis.source;

import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.bilibili.bsql.common.api.function.AsyncLookupFunctionBase;
import com.bilibili.bsql.common.cache.cacheobj.CacheContentType;
import com.bilibili.bsql.common.cache.cacheobj.CacheMissVal;
import com.bilibili.bsql.common.cache.cacheobj.CacheObj;
import com.bilibili.bsql.redis.BsqlRedisLookupOptionsEntity;
import com.bilibili.bsql.redis.RedisConstant;
import com.bilibili.bsql.redis.lettucecodec.ByteValueCodec;
import com.bilibili.bsql.redis.source.keyhandler.KeyGenerator;
import com.bilibili.bsql.redis.source.keyhandler.MultiStringKeyQuerier;
import com.bilibili.bsql.redis.source.keyhandler.SingleStringKeyQuerier;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.Partitions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.util.Preconditions.checkArgument;

/** @version $Id: RedisRowDataLookupFunction.java */
public class RedisLookupFunction extends AsyncLookupFunctionBase {
    private BsqlRedisLookupOptionsEntity options;

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private static final long serialVersionUID = -2079908694523987738L;

    private Duration queryTimeout;

    private RedisClusterClient clusterClient;

    private StatefulRedisClusterConnection clusterConnection;

    private RedisKeyAsyncCommands async;

    /** value type could be String or Byte[]. */
    private LogicalType valueType;

    /** key type could be String or String[]. */
    private Integer redisKeyIdx;

    private Integer redisValueIdx;

    public String delimitKey;

    public boolean isMultiKey;

    private boolean isRawOutput = false;

    private static final Map<String, Partitions> partitionsCache = new HashMap<>();

    private KeyGenerator keyGenerator;

    private Outputer outputer;

    private int maxRetries;

    public TimeOutHandle timeOutHandle;

    public RedisLookupFunction(
            BsqlRedisLookupOptionsEntity options,
            LookupTableSource.LookupContext context,
            DataType[] fieldTypes,
            RowType rowType) {
        super(options, context, rowType);
        checkArgument(
                keys.length == 1 && keys[0].length == 1,
                "redis side table do not support multi conditions !!!");
        checkArgument(
                this.dataType.getFieldCount() == 2,
                "redis side table do not support multi conditions !!!");

        checkArgument(options.keyIndex() == keys[0][0], "redis must use the key to join");
        this.options = options;
        this.redisKeyIdx = options.keyIndex();
        this.redisValueIdx = 1 - redisKeyIdx;
        this.valueType = fieldTypes[redisValueIdx].getLogicalType();

        this.delimitKey = options.multikeyDelimiter();
        this.isMultiKey = options.multiKey();
        this.maxRetries = options.retryMaxNum();
        this.queryTimeout = Duration.ofMillis(options.queryTimeOut());

        if (valueType instanceof ArrayType) {
            if (((ArrayType) valueType).getElementType().getTypeRoot() == VARBINARY) {
                this.isRawOutput = true;
            }
        } else if (valueType.getTypeRoot() == VARBINARY) {
            this.isRawOutput = true;
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        buildRedisClient(options);
        if (!isMultiKey) {
            if (isRawOutput) {
                keyGenerator = new SingleStringKeyQuerier<byte[]>((RedisStringAsyncCommands) async);
                outputer = new SingleBytesOutputer();
            } else {
                keyGenerator = new SingleStringKeyQuerier<String>((RedisStringAsyncCommands) async);
                outputer = new SingleStringOutputer();
            }
        } else {
            if (isRawOutput) {
                keyGenerator =
                        new MultiStringKeyQuerier<ArrayList<KeyValue<String, byte[]>>>(
                                this.delimitKey, (RedisStringAsyncCommands) async);
                outputer = new MultiBytesOutputer();
            } else {
                keyGenerator =
                        new MultiStringKeyQuerier<ArrayList<KeyValue>>(
                                this.delimitKey, (RedisStringAsyncCommands) async);
                outputer = new MultiStringOutputer();
            }
        }
        timeOutHandle = new TimeOutHandle(maxRetries, queryTimeout.toMillis());
    }

    private void buildRedisClient(BsqlRedisLookupOptionsEntity options) {
        String url = options.url();
        String password = options.password();

        switch (options.redisType()) {
            case RedisConstant.CLUSTER:
                String[] urls = url.split(",", -1);
                List<RedisURI> redisURIS = new ArrayList<RedisURI>(urls.length);
                for (String str : urls) {
                    RedisURI clusterURI = RedisURI.create("redis://" + str);
                    if (password != null) {
                        clusterURI.setPassword(password);
                    }
                    // clusterURI.setTimeout(DEFAULT_TIMEOUT);
                    redisURIS.add(clusterURI);
                }

                clusterClient = RedisClusterClient.create(redisURIS);
                if (partitionsCache.get(url) == null) {
                    synchronized (partitionsCache) {
                        if (partitionsCache.get(url) == null) {
                            Partitions partitions = clusterClient.getPartitions();
                            partitionsCache.put(url, partitions);
                            LOG.info("redis:{} fetch partitions:{}", url, partitions);
                        }
                    }
                }
                if (partitionsCache.get(url) == null) {
                    throw new RuntimeException("connect to redis fetch connection error");
                }
                clusterClient.setPartitions(partitionsCache.get(url));
                if (isRawOutput) {
                    clusterConnection = clusterClient.connect(ByteValueCodec.UTF8);
                } else {
                    clusterConnection = clusterClient.connect();
                }
                async = clusterConnection.async();
                clusterConnection.setTimeout(queryTimeout);
                break;
            default:
                throw new IllegalArgumentException("only support redis cluster");
        }
    }

    // todo: enable multikey join
    @Override
    public void queryFromTable(
            CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        if (timeOutHandle.isHasError()) {
            throw new RuntimeException(timeOutHandle.getError());
        }

        if (keyGenerator.shouldReturnDirectly(inputs)) {
            resultFuture.complete(null);
            return;
        }

        long start = System.nanoTime();
        final Object keyForSearch = keyGenerator.createKeyFromInput(inputs);
        if (keyForSearch instanceof Collection && ((Collection) keyForSearch).size() == 0) {
            resultFuture.complete(null);
            return;
        }

        // 重试异步延迟队列实现
        RedisFuture futureForValue = keyGenerator.queryRedis(keyForSearch);
        TimeOutFuture timeOutFuture =
                new TimeOutFuture(
                        keyGenerator,
                        futureForValue,
                        outputer,
                        resultFuture,
                        keyForSearch,
                        queryTimeout.toMillis(),
                        0);
        timeOutHandle.addTimeOutHandle(timeOutFuture);

        futureForValue.thenAccept(
                (Object redisValue) -> {
                    try {
                        outputer.handleRedisQuery(keyForSearch, resultFuture, start, redisValue);
                        // 移除超时定时器
                        timeOutFuture.getTimeOutScheduledFuture().cancel(true);
                    } catch (Throwable t) {
                        LOG.error("exception happened when redis join : ", t);
                        resultFuture.completeExceptionally(t);
                        throw t;
                    }
                });

        futureForValue.exceptionally(
                o -> timeOutHandle.handleException(o, timeOutFuture, inputs[0].toString()));
    }

    private class SingleStringOutputer implements Outputer<String, String> {
        public void handleRedisQuery(
                String keyInput,
                CompletableFuture<Collection<RowData>> resultFuture,
                long start,
                String redisValue) {
            lookupMetricsWrapper.rtQuerySide(start);
            if (redisValue != null) {
                GenericRowData returnRow =
                        doComplete(
                                StringData.fromString(keyInput),
                                StringData.fromString(redisValue),
                                resultFuture);
                if (enableCache()) {
                    putCache(
                            keyInput,
                            CacheObj.buildCacheObj(CacheContentType.SingleLine, returnRow));
                }
            } else {
                resultFuture.complete(null);
                if (enableCache()) {
                    putCache(keyInput, CacheMissVal.getMissKeyObj());
                }
            }
        }
    }

    private class SingleBytesOutputer implements Outputer<String, byte[]> {
        public void handleRedisQuery(
                String keyInput,
                CompletableFuture<Collection<RowData>> resultFuture,
                long start,
                byte[] redisValue) {
            lookupMetricsWrapper.rtQuerySide(start);
            if (redisValue != null) {
                GenericRowData returnRow =
                        doComplete(StringData.fromString(keyInput), redisValue, resultFuture);
                if (enableCache()) {
                    putCache(
                            keyInput,
                            CacheObj.buildCacheObj(CacheContentType.SingleLine, returnRow));
                }
            } else {
                resultFuture.complete(null);
                if (enableCache()) {
                    putCache(keyInput, CacheMissVal.getMissKeyObj());
                }
            }
        }
    }

    private class MultiStringOutputer implements Outputer<String[], ArrayList<KeyValue>> {
        public void handleRedisQuery(
                String[] keyInput,
                CompletableFuture<Collection<RowData>> resultFuture,
                long start,
                ArrayList<KeyValue> redisValue) {
            lookupMetricsWrapper.rtQuerySide(start);
            if (redisValue != null) {
                /**
                 * make sure the value and key has the same size; if the key is null, the converted
                 * key will omit the
                 */
                //				assert keyInput.length == redisValue.size();

                StringData[] resultList = new StringData[keyInput.length];
                int j = 0;
                for (int i = 0; i < keyInput.length; i++) {
                    if (StringUtils.isNotEmpty(keyInput[i])) {
                        resultList[i] =
                                StringData.fromString(
                                        redisValue.get(j).hasValue()
                                                ? (String) redisValue.get(j).getValue()
                                                : "");
                        j++;
                    } else {
                        resultList[i] = StringData.fromString("");
                    }
                }
                doComplete(
                        StringData.fromString(
                                StringUtils.join(keyInput, RedisLookupFunction.this.delimitKey)),
                        new GenericArrayData(resultList),
                        resultFuture);
            } else {
                resultFuture.complete(null);
            }
        }
    }

    /**
     * @version com.bilibili.bsql.redis.source.RedisLookupFunction.MultiBytesOutputer
     * @see Outputer
     */
    private class MultiBytesOutputer
            implements Outputer<String[], ArrayList<KeyValue<String, byte[]>>> {
        public void handleRedisQuery(
                String[] keyInput,
                CompletableFuture<Collection<RowData>> resultFuture,
                long start,
                ArrayList<KeyValue<String, byte[]>> redisValue) {
            lookupMetricsWrapper.rtQuerySide(start);
            if (redisValue != null) {
                List<byte[]> resultArray = new ArrayList<>(keyInput.length);
                int j = 0;
                for (String s : keyInput) {
                    if (StringUtils.isNotEmpty(s)) {
                        resultArray.add(
                                redisValue.get(j).hasValue()
                                        ? redisValue.get(j).getValue()
                                        : new byte[0]);
                        j++;
                    } else {
                        resultArray.add(new byte[0]);
                    }
                }
                doComplete(
                        StringData.fromString(
                                StringUtils.join(keyInput, RedisLookupFunction.this.delimitKey)),
                        new GenericArrayData(resultArray.toArray()),
                        resultFuture);
            } else {
                resultFuture.complete(null);
            }
        }
    }

    private GenericRowData doComplete(
            Object key, Object value, CompletableFuture<Collection<RowData>> resultFuture) {
        GenericRowData returnRow = new GenericRowData(2);
        returnRow.setField(RedisLookupFunction.this.redisKeyIdx, key);
        returnRow.setField(RedisLookupFunction.this.redisValueIdx, value);

        lookupMetricsWrapper.sideJoinSuccess();
        resultFuture.complete(Collections.singleton(returnRow));
        return returnRow;
    }

    /** @version com.bilibili.bsql.redis.source.RedisLookupFunction.Outputer */
    public interface Outputer<K, V> {
        void handleRedisQuery(
                K keyInput,
                CompletableFuture<Collection<RowData>> resultFuture,
                long start,
                V redisValue);
    }

    @Override
    public String createCacheKey(Object... inputs) {
        return keyGenerator.createCacheKey(inputs);
    }

    @Override
    public void close() throws Exception {
        if (clusterConnection != null) {
            clusterConnection.close();
        }
        if (clusterClient != null) {
            clusterClient.shutdown();
        }

        if (timeOutHandle != null) {
            timeOutHandle.shutDown();
        }
    }
}
