package com.bilibili.bsql.taishan.lookup;

import com.bapis.infra.service.taishan.BatchGetResp;
import com.bapis.infra.service.taishan.Column;
import com.bapis.infra.service.taishan.GetResp;
import com.bilibili.bsql.common.cache.cachobj.CacheContentType;
import com.bilibili.bsql.common.cache.cachobj.CacheMissVal;
import com.bilibili.bsql.common.cache.cachobj.CacheObj;
import com.bilibili.bsql.common.function.AsyncSideFunction;
import com.bilibili.bsql.taishan.format.TaishanRPC;
import com.bilibili.bsql.taishan.lookup.keyhandler.KeyHandler;
import com.bilibili.bsql.taishan.lookup.keyhandler.MultipleKeyHandler;
import com.bilibili.bsql.taishan.lookup.keyhandler.SingleKeyHandler;
import com.bilibili.bsql.taishan.tableinfo.TaishanSideTableInfo;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.grpc.Status.Code.DEADLINE_EXCEEDED;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;

public class TaishanLookupFunction extends AsyncSideFunction<TaishanSideTableInfo> {

    private final static Logger LOG = LoggerFactory.getLogger(TaishanLookupFunction.class);

    private static final long serialVersionUID = 1580884495664873020L;

    private TaishanRPC taishanRPC;
    private final String delimitKey;
    private final boolean isMultiKey;
    private final Integer redisKeyIdx;
    private final Integer redisValueIdx;
    private boolean isRawOutput = false;
    private final long timeout;
    private volatile RuntimeException cancelException;

    private transient KeyHandler keyHandler;
    private transient ListeningExecutorService pool;

    public TaishanLookupFunction(TaishanSideTableInfo sideTableInfo, LookupTableSource.LookupContext context) {
        super(sideTableInfo, context);
        this.delimitKey = sideTableInfo.getMultiKeyDelimitor();
        this.isMultiKey = sideTableInfo.isMultiKey();
        this.redisKeyIdx = sideTableInfo.getKeyIndex();
        this.redisValueIdx = this.redisKeyIdx > 0 ? 0 : 1;
        LogicalType valueType = sideTableInfo.getPhysicalFields().get(redisValueIdx).getType();
        this.timeout = sideTableInfo.getAsyncTimeout();
        if (valueType instanceof ArrayType) {
            if (((ArrayType) valueType).getElementType().getTypeRoot() == VARBINARY) {
                this.isRawOutput = true;
            }
        } else if (valueType.getTypeRoot() == VARBINARY) {
            this.isRawOutput = true;
        }
    }

    @Override
    protected void open0(FunctionContext context) {
		taishanRPC = new TaishanRPC(sideTableInfo.getZone(), sideTableInfo.getCluster(),
			sideTableInfo.getKeepAliveTime(), sideTableInfo.getKeepAliveTimeout(), sideTableInfo.getIdleTimeout());
        if (isMultiKey) {
            keyHandler = new MultipleKeyHandler(delimitKey, taishanRPC, sideTableInfo.getTable(),
                    sideTableInfo.getPassword(), timeout);
        } else {
            keyHandler = new SingleKeyHandler(taishanRPC, sideTableInfo.getTable(), sideTableInfo.getPassword(),
                    timeout);
        }
        //keepAliveTime足够长，或者 保证core 和max 线程数一致，避免产生过多的线程metric
		pool = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(1, 100,
			1L, TimeUnit.DAYS, new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy()));
        LOG.info("Taishan side function init completed!");
    }

    @Override
    protected void close0() {
        taishanRPC.close();
    }

    @Override
    protected void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        if (cancelException != null) {
            throw cancelException;
        }
        if (keyHandler.shouldReturnDirectly(inputs)) {
            resultFuture.complete(null);
            return;
        }
        Object req = keyHandler.buildReq(inputs);
        long start = System.nanoTime();
        //add async callback
        Futures.addCallback(keyHandler.query(req), new QueryCallback(inputs, req, resultFuture, start, 0), pool);
    }

    @Override
    protected String createCacheKey(Object... inputs) {
        return keyHandler.createCacheKey(inputs);
    }

    private void handleQueryResult(Object[] inputs, CompletableFuture<Collection<RowData>> resultFuture,
                                   long start, Object queryValue) {
        sideMetricsWrapper.rtQuerySide(start);
        GenericRowData returnRow = new GenericRowData(2);
        returnRow.setField(TaishanLookupFunction.this.redisKeyIdx, StringData.fromString(inputs[0].toString()));
        if (isMultiKey) {
            BatchGetResp result = (BatchGetResp) queryValue;
            List<Column> cols = result.getRecordsList().stream()
                    .map(record -> record.getColumns(0))
                    .collect(Collectors.toList());
            if (isRawOutput) {
                List<byte[]> vals = new ArrayList<>(cols.size());
                for (Column col : cols) {
                    if (col == null) {
                        vals.add(new byte[0]);
                    } else {
                        vals.add(col.toByteArray());
                    }
                }
                returnRow.setField(TaishanLookupFunction.this.redisValueIdx, new GenericArrayData(vals.toArray()));
            } else {
                StringData[] vals = new StringData[cols.size()];
                for (int i = 0; i < cols.size(); i++) {
                    if (cols.get(i) == null) {
                        vals[i] = StringData.fromString("");
                    } else {
                        vals[i] = StringData.fromBytes(cols.get(i).getValue().toByteArray());
                    }
                }
                returnRow.setField(TaishanLookupFunction.this.redisValueIdx, new GenericArrayData(vals));
            }
        } else {
            GetResp result = (GetResp) queryValue;
            if (result.getRecord().getColumns(0).getValue() == ByteString.EMPTY) {
                resultFuture.complete(null);
                if (openCache()) {
                    putCache(createCacheKey(inputs), CacheMissVal.getMissKeyObj());
                }
                return;
            }
            if (isRawOutput) {
                byte[] vals = result.getRecord().getColumns(0).getValue().toByteArray();
                returnRow.setField(TaishanLookupFunction.this.redisValueIdx, vals);
            } else {
                StringData vals = StringData.fromBytes(result.getRecord().getColumns(0).getValue().toByteArray());
                returnRow.setField(TaishanLookupFunction.this.redisValueIdx, vals);
            }
            if (openCache()) {
                putCache(createCacheKey(inputs), CacheObj.buildCacheObj(CacheContentType.SingleLine, returnRow));
            }
        }
        sideMetricsWrapper.sideJoinSuccess();
        resultFuture.complete(Collections.singleton(returnRow));
    }

    class QueryCallback implements FutureCallback<Object> {

        Object[] inputs;
        Object request;
        CompletableFuture<Collection<RowData>> resultFuture;
        long start;
        int retry;

        public QueryCallback(Object[] inputs, Object request,
                             CompletableFuture<Collection<RowData>> resultFuture,
                             long start, int retry) {
            this.inputs = inputs;
            this.request = request;
            this.resultFuture = resultFuture;
            this.start = start;
            this.retry = retry;
        }

        @Override
        public void onSuccess(@Nullable Object value) {
            handleQueryResult(inputs, resultFuture, start, value);
        }

        @Override
        public void onFailure(Throwable throwable) {
            //retry
            if (throwable instanceof StatusRuntimeException) {
                StatusRuntimeException e = (StatusRuntimeException) throwable;
                LOG.warn("taishan query timeout, cost: {}, key: {}, retry times: {}, StatusRuntimeException: {}, code: {}", System.nanoTime() - start,
                        ArrayUtils.toString(inputs), retry, e.getMessage(), e.getStatus(), e.getCause());
            } else {
                LOG.warn("taishan query timeout, cost: {}, key: {}, retry times: {}, exception: {}", System.nanoTime() - start,
                        ArrayUtils.toString(inputs), retry, throwable.getMessage());
            }
            Futures.addCallback(keyHandler.query(request), new QueryCallback(inputs, request, resultFuture, start, ++retry),
                    pool);
            sideMetricsWrapper.rpcTimeout();
        }
    }

}
