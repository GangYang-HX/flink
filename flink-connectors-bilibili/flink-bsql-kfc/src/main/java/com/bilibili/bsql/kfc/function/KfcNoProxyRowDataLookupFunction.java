/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kfc.function;

import com.bilibili.ai.athena.*;
import com.bilibili.bsql.common.cache.cachobj.CacheContentType;
import com.bilibili.bsql.common.cache.cachobj.CacheObj;
import com.bilibili.bsql.common.function.AsyncSideFunction;
import com.bilibili.bsql.kfc.tableinfo.KfcSideTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @author jie
 * @version $Id: KfcCDRowDataLookupFunction.java, v 0.1 2022-02-21 20:25
 * jie Exp $$
 */
public class KfcNoProxyRowDataLookupFunction extends AsyncSideFunction<KfcSideTableInfo> implements ProcessingTimeCallback {

    private final static Logger LOG = LoggerFactory.getLogger(KfcNoProxyRowDataLookupFunction.class);

    private final static Integer DEFAULT_POOL_SIZE = 3;
    private final static Integer MAX_POOL_SIZE = 5;

    private boolean isRawOutput = false;

    public Integer KeyIdx;

    public Integer ValueIdx;

    public String delimitKey;

    public boolean isMultiKey;

    protected static ValidateSideMetricsWrapper sideMetricsWrapper;

    private static final String uid = UUID.randomUUID().toString();

    private int batchSize = 0;

    private final ArrayList<String> joinKeys;

    private final Map<CompletableFuture<Collection<RowData>>, String[]> resultFutureAndKeys;

    private final long batchTimeMillis;

    private ProcessingTimeService processingTimeService;

    public static final Object lockForKfcInit = new Object();

    public static final AtomicBoolean atomicBooleanForKfcInit = new AtomicBoolean(false);

    public KfcNoProxyRowDataLookupFunction(KfcSideTableInfo sideTableInfo, LookupTableSource.LookupContext context) {
        super(sideTableInfo, context);
        checkArgument(this.dataType.getFieldCount() == 2, "kfc side table do not support multi conditions !!!");
        checkArgument(keys.length == 1, "kfc side table do not support multi conditions !!!");
        checkArgument(sideTableInfo.getKeyIndex() == keys[0][0], "kfc must use the key to join");

        this.batchTimeMillis = sideTableInfo.getBatchTimeMillis();
        this.batchSize = sideTableInfo.getBatchSize();
        this.joinKeys = new ArrayList<>(batchSize * 2);
        this.resultFutureAndKeys = new HashMap<>(batchSize);
        this.KeyIdx = sideTableInfo.getKeyIndex();
        this.ValueIdx = this.KeyIdx > 0 ? 0 : 1;
        this.delimitKey = sideTableInfo.getMultiKeyDelimitor();
        this.isMultiKey = sideTableInfo.isMultiKey();

        LogicalType valueType = sideTableInfo.getPhysicalFields().get(ValueIdx).getType();

        if (valueType instanceof ArrayType) {
            if (((ArrayType) valueType).getElementType().getTypeRoot() == VARBINARY) {
                this.isRawOutput = true;
            }
        } else if (valueType.getTypeRoot() == VARBINARY) {
            this.isRawOutput = true;
        }
    }

    @Override
    public void open0(FunctionContext context) throws Exception {
        RuntimeContext runtimeContext = context.getContext();
        int asyncBufferCapacity = Integer.parseInt(runtimeContext.getExecutionConfig().getGlobalJobParameters().toMap().get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY.key()));
        checkArgument(batchSize < asyncBufferCapacity, "The batchSize overhead the max number " + asyncBufferCapacity + " of async i/o operation that the kfc async lookup join can trigger");
        if (runtimeContext instanceof RichAsyncFunction.RichAsyncFunctionRuntimeContext) {
            RichAsyncFunction.RichAsyncFunctionRuntimeContext rafrc = (RichAsyncFunction.RichAsyncFunctionRuntimeContext) runtimeContext;
            RichAsyncFunction.RichAsyncFunctionRuntimeContext rafrc2 = (RichAsyncFunction.RichAsyncFunctionRuntimeContext) rafrc.getRuntimeContext();
            StreamingRuntimeContext src = (StreamingRuntimeContext) rafrc2.getRuntimeContext();
            this.processingTimeService = src.getProcessingTimeService();
        }

        if (batchTimeMillis > 0 && batchSize > 1) {
            processingTimeService.registerTimer(processingTimeService.getCurrentProcessingTime() + batchTimeMillis, this);
        }

        initKfcClient(context);
    }

    private void initKfcClient(FunctionContext context) {
        synchronized (lockForKfcInit) {
            if (!atomicBooleanForKfcInit.compareAndSet(false, true)) {
                return;
            }
            sideMetricsWrapper = new ValidateSideMetricsWrapper(sideTableInfo.getType(), context);
            int c = Runtime.getRuntime().availableProcessors();
            String tmpDir = System.getenv("LOCAL_DIRS");
            LOG.info("tmpDir tmp path: {}", tmpDir);
            String logDir;
            if (tmpDir == null) {
                logDir = System.getProperty("java.io.tmpdir");
            } else {
                String[] localDirArray = tmpDir.split(",");
                Random random = new Random();
                logDir = localDirArray[random.nextInt(localDirArray.length)];
            }
            logDir = logDir.endsWith(File.separator) ? logDir + "kfc/log/" : logDir + File.separator + "kfc/log";

            PetaConfiguration configuration = PetaConfiguration.newBuilder()
                    .defaultPoolSize(c * DEFAULT_POOL_SIZE)
                    .maxPoolSize(c * MAX_POOL_SIZE)
                    .logDir(logDir)
                    .runModeAuto()
					.kfcAddr(getZone(sideTableInfo.getZone()))
                    .build();
            LOG.info("kfc config ：{}", configuration.toString());
            LOG.info("log tmp path: {}", logDir);
            PetaJniClient.Init(configuration);
            LOG.info("poolExecutor init success : {}", PetaJniClient.poolExecutor.toString());
        }
    }

	private String getZone(String zone) {
		switch (zone.toLowerCase()) {
			case "suzhou":
				return PetaConfiguration.KFC_ADDR_SUZHOU;
			case "changshu":
				return PetaConfiguration.KFC_ADDR_CHANGSHU;
			default:
				throw new UnsupportedOperationException("Zone only support Suzhou or Changshu");
		}
	}

	@Override
	public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
		super.sideMetricsWrapper.tps();
		eval0(resultFuture, inputs);
	}

	@Override
    public void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
        sideMetricsWrapper.tps();

        if(inputs[0] == null){
            resultFuture.complete(null);
            return;
        }

        String originKey = inputs[0].toString();
        if (StringUtils.isEmpty(originKey)) {
            resultFuture.complete(null);
            return;
        }

        ArrayList<String> singleKeys;
        if (!isMultiKey) {
            joinKeys.add(originKey);
            singleKeys = new ArrayList<>(1);
            singleKeys.add(originKey);
        } else {
            String[] inputSplit = StringUtils.splitPreserveAllTokens(originKey, this.delimitKey);
            singleKeys = new ArrayList<>(inputSplit.length);
            for (String field : inputSplit) {
                if (StringUtils.isEmpty(field)) {
                    continue;
                }
                joinKeys.add(field);
                singleKeys.add(field);
            }
			checkInCache(resultFuture, singleKeys);
        }
        this.resultFutureAndKeys.put(resultFuture, singleKeys.toArray(new String[singleKeys.size()]));
        if (joinKeys.size() >= batchSize) {
            eval();
        }
    }

    private void eval() {
        if (joinKeys.size() == 0) {
            return;
        }
        //copy
        ArrayList<String> keyList = new ArrayList<>(joinKeys);
        BatchKfcCallback batchKfcCallback;
        if (!isMultiKey) {
            batchKfcCallback = new BatchSingleKeyCallback(new HashMap<>(this.resultFutureAndKeys), isRawOutput);
        } else {
            batchKfcCallback = new BatchMultiKeyCallback(new HashMap<>(this.resultFutureAndKeys), isRawOutput);
        }
        try {
            PetaJniClient.GetValues(uid, keyList, batchKfcCallback);
            this.joinKeys.clear();
            this.resultFutureAndKeys.clear();
        } catch (Exception e) {
            this.resultFutureAndKeys.keySet().forEach(rf -> rf.complete(null));
            LOG.error(e.toString());
            throw e;
        }
    }

	private void checkInCache(CompletableFuture<Collection<RowData>> resultFuture, ArrayList<String> singleKeys){
		if (openCache()) {
			long start = System.nanoTime();
			GenericRowData rowData = new GenericRowData(2);
			rowData.setField(KfcNoProxyRowDataLookupFunction.this.KeyIdx, StringData.fromString(String.join(KfcNoProxyRowDataLookupFunction.this.delimitKey, singleKeys)));
			boolean allDone = true;
			if (isRawOutput) {
				handleRawOutput(resultFuture, allDone, singleKeys, rowData, start);
			} else {
				handleNotRawOutput(resultFuture, allDone, singleKeys, rowData, start);
			}
		}
	}
	private void handleRawOutput(CompletableFuture<Collection<RowData>> resultFuture, boolean allDone, ArrayList<String> singleKeys, GenericRowData rowData, long start){
		byte[][] resultList = new byte[singleKeys.size()][];
		for (int i = 0; i < singleKeys.size(); i++) {
			CacheObj val = getFromCache(singleKeys.get(i));
			if (val != null) {
				if (CacheContentType.MissVal == val.getType()) {
					/**
					 * miss value does not mean not found in cache, but means in cache but not in external system
					 * */
					resultFuture.complete(null);
					sideMetricsWrapper.sideJoinSuccess();
				} else if (CacheContentType.SingleLine == val.getType()) {
					GenericRowData genericRowData = (GenericRowData) val.getContent();
					byte[] content = (byte[]) genericRowData.getField(0);
					LOG.debug("byte[] get from cache, length is {}", content.length);
					resultList[i] = content;
				} else {
					throw new RuntimeException("not support cache obj type " + val.getType());
				}
			} else {
				allDone = false;
				LOG.debug("query key is not in cache!");
				break;
			}
		}
		if (allDone){
			rowData.setField(KfcNoProxyRowDataLookupFunction.this.ValueIdx, new GenericArrayData(resultList));
			sideMetricsWrapper.sideJoinSuccess();
			sideMetricsWrapper.rtQuerySide(start);
			LOG.debug("cache rowData is {}", rowData.toString());
			resultFuture.complete(Collections.singleton(rowData));
		}
	}

	private void handleNotRawOutput(CompletableFuture<Collection<RowData>> resultFuture, boolean allDone, ArrayList<String> singleKeys, GenericRowData rowData, long start){
		StringData[] resultList = new StringData[singleKeys.size()];
		for (int i = 0; i < singleKeys.size(); i++) {
			CacheObj val = getFromCache(singleKeys.get(i));
			if (val != null) {
				if (CacheContentType.MissVal == val.getType()) {
					/**
					 * miss value does not mean not found in cache, but means in cache but not in external system
					 * */
					resultFuture.complete(null);
					sideMetricsWrapper.sideJoinSuccess();
				} else if (CacheContentType.SingleLine == val.getType()) {
					GenericRowData content = (GenericRowData) val.getContent();
					String str = (String) content.getField(0);
					StringData stringData = StringData.fromString(str);
					LOG.debug("stringData get from cache is {}", stringData.toString());
					resultList[i] = stringData;
				} else {
					throw new RuntimeException("not support cache obj type " + val.getType());
				}
			} else {
				allDone = false;
				LOG.debug("query key is not in cache!");
				break;
			}
		}
		if (allDone){
			rowData.setField(KfcNoProxyRowDataLookupFunction.this.ValueIdx, new GenericArrayData(resultList));
			sideMetricsWrapper.sideJoinSuccess();
			sideMetricsWrapper.rtQuerySide(start);
			LOG.debug("cache rowData is {}", rowData.toString());
			resultFuture.complete(Collections.singleton(rowData));
		}
	}

    @Override
    public String createCacheKey(Object... inputs) {
        throw new UnsupportedOperationException("kfc does not create cache");
    }

    @Override
    public void close0() {
    }

    @Override
    public void onProcessingTime(long timestamp) {
        eval();
        processingTimeService.registerTimer(processingTimeService.getCurrentProcessingTime() + batchTimeMillis, this);
    }

    public final class BatchMultiKeyCallback extends BatchKfcCallback {

        public BatchMultiKeyCallback(Map<CompletableFuture<Collection<RowData>>, String[]> resultFutureAndKeys, boolean rawInput) {
            super(resultFutureAndKeys, rawInput);
        }

        @Override
        public void success(PetaCppResp response) {
            try {
                sideMetricsWrapper.sideJoinSuccess();
                Map<String, Result> resultMap = response.getResults();
                if (resultMap == null || resultMap.size() == 0) {
                    LOG.info("getValues response is null");
                    sideMetricsWrapper.sideJoinSuccess();
                    rfAndK.keySet().forEach(cf -> {
                        cf.complete(null);
                    });
                } else {
                    sideMetricsWrapper.rtQuerySide(startTime);
                    GenericRowData rowData;
                    Set<Map.Entry<CompletableFuture<Collection<RowData>>, String[]>> entries = super.rfAndK.entrySet();
                    for (Map.Entry<CompletableFuture<Collection<RowData>>, String[]> entry : entries) {
                        CompletableFuture<Collection<RowData>> future = entry.getKey();
                        if (future.isDone()) {
                            continue;
                        }
                        String[] originKey = entry.getValue();
                        rowData = new GenericRowData(2);
                        rowData.setField(KfcNoProxyRowDataLookupFunction.this.KeyIdx, StringData.fromString(String.join(KfcNoProxyRowDataLookupFunction.this.delimitKey, originKey)));
                        fillInResponseValue(resultMap, rowData, originKey);
						if (openCache()) {
							LOG.debug("try to put data into cache!");
							for (String s : originKey) {
								if (rawInput) {
									byte[] bytes = parseRawValue(resultMap.get(s));
									LOG.debug("byte[] length is {}", bytes.length);
									if (bytes.length != 0) {
										GenericRowData genericRowData = new GenericRowData(1);
										genericRowData.setField(0, bytes);
										LOG.debug("put into cache, key is {}", s);
										putCache(s, CacheObj.buildCacheObj(CacheContentType.SingleLine, genericRowData));
									}
								} else {
									String str = parseValue(resultMap.get(s));
									if (str.length() != 0) {
										GenericRowData genericRowData = new GenericRowData(1);
										genericRowData.setField(0, str);
										LOG.debug("put into cache, key is {}", s);
										putCache(s, CacheObj.buildCacheObj(CacheContentType.SingleLine, genericRowData));
									}
								}
							}
						}
						LOG.debug("future rowData is {}", rowData.toString());
						future.complete(Collections.singleton(rowData));
                    }
                }
                super.rfAndK.clear();
                validationResultReport(response);
            } catch (Exception e) {
                fail(e);
            }
        }


        public void fillInResponseValue(Map<String, Result> resultMap, GenericRowData rowData, String[] originKey) {
            if (rawInput) {
                byte[][] resultList = new byte[originKey.length][];
                for (int i = 0; i < originKey.length; i++) {
                    resultList[i] = parseRawValue(resultMap.get(originKey[i]));
                }
                rowData.setField(KfcNoProxyRowDataLookupFunction.this.ValueIdx, new GenericArrayData(resultList));
            } else {
                StringData[] resultList = new StringData[originKey.length];
                for (int i = 0; i < originKey.length; i++) {
                    resultList[i] = StringData.fromString(parseValue(resultMap.get(originKey[i])));
                }
                rowData.setField(KfcNoProxyRowDataLookupFunction.this.ValueIdx, new GenericArrayData(resultList));
            }
        }
    }

    public final class BatchSingleKeyCallback extends BatchKfcCallback {

        public BatchSingleKeyCallback(Map<CompletableFuture<Collection<RowData>>, String[]> resultFutureAndKeys, boolean rawInput) {
            super(resultFutureAndKeys, rawInput);
        }

        @Override
        public void success(PetaCppResp response) {
            try {
                sideMetricsWrapper.sideJoinSuccess();
                Map<String, Result> resultMap = response.getResults();
                if (resultMap == null || resultMap.size() == 0) {
                    LOG.info("getValues response is null");
                    sideMetricsWrapper.sideJoinSuccess();
                    super.rfAndK.keySet().forEach(cf -> {
                        cf.complete(null);
                    });
                } else {
                    sideMetricsWrapper.rtQuerySide(startTime);
                    GenericRowData rowData;
                    Set<Map.Entry<CompletableFuture<Collection<RowData>>, String[]>> entries = super.rfAndK.entrySet();
                    for (Map.Entry<CompletableFuture<Collection<RowData>>, String[]> entry : entries) {
                        CompletableFuture<Collection<RowData>> future = entry.getKey();
                        if (future.isDone()) {
                            continue;
                        }
                        String[] originKey = entry.getValue();
                        rowData = new GenericRowData(2);
                        rowData.setField(KfcNoProxyRowDataLookupFunction.this.KeyIdx, StringData.fromString(originKey[0]));
                        fillInResponseValue(resultMap, rowData, originKey);
                        future.complete(Collections.singleton(rowData));
                    }
                }
                validationResultReport(response);
                super.rfAndK.clear();
            } catch (Exception e) {
                fail(e);
            }
        }

        @Override
        public void fillInResponseValue(Map<String, Result> resultMap, GenericRowData rowData, String[] originKey) {
            String SingleKey = originKey[0];
            if (rawInput) {
                byte[] result = parseRawValue(resultMap.get(SingleKey));
                rowData.setField(KfcNoProxyRowDataLookupFunction.this.ValueIdx, result);
            } else {
                String result = parseValue(resultMap.get(SingleKey));
                rowData.setField(KfcNoProxyRowDataLookupFunction.this.ValueIdx, StringData.fromString(result));
            }
        }
    }

    public abstract class BatchKfcCallback implements PetaCallback {

        protected long batch = 0;

        protected final long startTime = System.nanoTime();

        protected boolean rawInput;
        protected final Map<CompletableFuture<Collection<RowData>>, String[]> rfAndK;

        public BatchKfcCallback(Map<CompletableFuture<Collection<RowData>>, String[]> rfAndK, boolean rawInput) {
            this.rfAndK = rfAndK;
            this.rawInput = rawInput;
        }

        @Override
        public abstract void success(PetaCppResp response);

        @Override
        public void fail(Throwable e) {
            LOG.info("batch {} is fail,{}", this.batch, e.getMessage());
            sideMetricsWrapper.rpcCallbackFailure();
            rfAndK.keySet().forEach(cf -> {
                if (!cf.isDone()) {
                    cf.complete(null);
                }
            });
            rfAndK.clear();
        }

        public abstract void fillInResponseValue(Map<String, Result> resultMap, GenericRowData rowData, String[] originKey);

        //parse
        public String parseValue(Result result) {
            if (result == null) {
                return "";
            }
            return result.HasError() ? "" : new String(result.getValues(), StandardCharsets.UTF_8);
        }

        public byte[] parseRawValue(Result result) {
            if (result == null || result.HasError()) {
                return new byte[0];
            }
            return result.getValues();
        }

        public void validationResultReport(PetaCppResp response) {
            try {
                ArrayList<Validation> validationResults = response.getValidationResults();
                for (Validation vr : validationResults) {
                    String prefix = vr.getPrefix();
                    if (StringUtils.isBlank(prefix)) {
                        continue;
                    }
                    String[] split = prefix.split(":");
                    prefix = split[0];
                    sideMetricsWrapper.validateMetric("equal", prefix, vr.getEqual());
                    sideMetricsWrapper.validateMetric("k3_not_found", prefix, vr.getK3NotFound());
                    sideMetricsWrapper.validateMetric("k3_unknown_error", prefix, vr.getK3UnknownError());
                    sideMetricsWrapper.validateMetric("proxy_not_found", prefix, vr.getProxyNotFound());
                    sideMetricsWrapper.validateMetric("value_not_equal", prefix, vr.getValueNotEqual());
                }
            } catch (Exception e) {
                LOG.error("kfc validation metrics error, {}", e.toString());
            }

        }
    }

}
