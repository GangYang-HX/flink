/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.kfc.function;

import ai.Trace;
import ai.kfc.KfcMessage;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcCallback;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.interceptor.Interceptor;
import com.baidu.brpc.protocol.Options;
import com.baidu.brpc.spi.ExtensionLoaderManager;
import com.bilibili.bsql.common.function.AsyncSideFunction;
import com.bilibili.bsql.kfc.proxy.KfcServiceAsync;
import com.bilibili.bsql.kfc.tableinfo.KfcSideTableInfo;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
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

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.Charset.forName;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 *
 * @author zhouxiaogang
 * @version $Id: KfcRowDataLookupFunction.java, v 0.1 2020-10-07 20:25
zhouxiaogang Exp $$
 */
public class KfcRowDataLookupFunction extends AsyncSideFunction<KfcSideTableInfo> {

	private final static Logger LOG              = LoggerFactory.getLogger(KfcRowDataLookupFunction.class);

	private final Integer DEFAULT_POOL_SIZE = 3;
	private final Integer MAX_POOL_SIZE = 5;

	private KfcServiceAsync asyncKfcService;

	private RpcClient rpcClient;

	private boolean isRawOutput = false;

	private LogicalType valueType;

	/**
	 * Varchar: String not null
	 * ARRAY<STRING>: ArrayType: VarCharType
	 * ARRAY<BYTES>:
	 * */
	private LogicalType keyType;

	public Integer KeyIdx;

	public Integer ValueIdx;

	public String delimitKey;

	public boolean isMultiKey;


	public KfcRowDataLookupFunction(KfcSideTableInfo sideTableInfo, LookupTableSource.LookupContext context) {
		super(sideTableInfo, context);

		checkArgument(this.dataType.getFieldCount() == 2, "kfc side table do not support multi conditions !!!");
		checkArgument(keys.length == 1 ,"kfc side table do not support multi conditions !!!");
		checkArgument(sideTableInfo.getKeyIndex() == keys[0][0], "kfc must use the key to join");
        checkArgument(sideTableInfo.getServiceUrl().size() > 0 , "url must be defined");

        this.KeyIdx = sideTableInfo.getKeyIndex();
		this.ValueIdx = this.KeyIdx > 0 ? 0 : 1;
		this.keyType = sideTableInfo.getPhysicalFields().get(KeyIdx).getType();
		this.valueType = sideTableInfo.getPhysicalFields().get(ValueIdx).getType();

		this.delimitKey = sideTableInfo.getMultiKeyDelimitor();
		this.isMultiKey = sideTableInfo.isMultiKey();

		if (valueType instanceof ArrayType) {
			if (((ArrayType)valueType).getElementType().getTypeRoot() == VARBINARY) {
				this.isRawOutput = true;
			}
		} else if (valueType.getTypeRoot() == VARBINARY) {
            this.isRawOutput = true;
        }
	}

	@Override
	public void open0(FunctionContext context) throws Exception {
		RpcClientOptions clientOption = new RpcClientOptions();
		clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
		clientOption.setWriteTimeoutMillis(1000);
		clientOption.setReadTimeoutMillis(5000);
		clientOption.setMaxTotalConnections(50);
		clientOption.setMinIdleConnections(10);
		clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_ROUND_ROBIN);
		clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
		clientOption.setIoThreadNum(3);
		clientOption.setWorkThreadNum(1);
		clientOption.setGlobalThreadPoolSharing(false);

		String serviceUrl = sideTableInfo.getServiceUrl().get(0);
		List<Interceptor> interceptors = new ArrayList<Interceptor>();

		//重试次数应该和list的长度保持一致
		clientOption.setMaxTryTimes(Integer.max(serviceUrl.split(",").length, sideTableInfo.getRetryTimes()));

		ExtensionLoaderManager.getInstance().loadAllExtensions(clientOption.getEncoding());
		Thread.sleep(1500);

		rpcClient = new RpcClient(serviceUrl, clientOption, interceptors);
		rpcClient.setLoadBalanceInterceptor(new BiliLoadBalanceInterceptor(sideMetricsWrapper));
		asyncKfcService = BrpcProxy.getProxy(rpcClient, KfcServiceAsync.class);

	}


	@Override
	public void eval0(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {

		sideMetricsWrapper.tps();
		KfcMessage.KFCRequest.Builder messageBuilder = KfcMessage.KFCRequest.newBuilder();

		Trace.TraceArg.Builder traceBuilder = Trace.TraceArg.newBuilder();
		traceBuilder.setTaskId(ByteString.copyFromUtf8(sideTableInfo.getJobId()));
		/**
         * set baseline label for tag 35
         **/
		if (sideTableInfo.getJobTags() != null) {
			for (Object tag: sideTableInfo.getJobTags()) {
				if ("35".equals(tag) ) {
					traceBuilder.setIsBaseline(true);
				}
			}
		}

		messageBuilder.setTrace(traceBuilder.build());

        KfcCallback kfcCallback;
        String originKey = inputs[0].toString();

		if(originKey == null){
			resultFuture.complete(null);
			return;
		}

		if(!isMultiKey) {
			messageBuilder.addKeys(originKey);
            kfcCallback = new SingleKeyCallback(resultFuture,
                    isMultiKey,
                    originKey,
                    isRawOutput);

		} else if (isMultiKey) {
		    String[] inputSplit = StringUtils.splitPreserveAllTokens(originKey, this.delimitKey);

			for (String field : inputSplit){
				if( field==null ){
					continue;
				}
				messageBuilder.addKeys(field);
			}
            kfcCallback = new MultiKeyCallback(resultFuture,
                    isMultiKey,
                    inputSplit,
                    isRawOutput);
		} else {
			resultFuture.complete(null);
			return;
		}

		try{
			asyncKfcService.GetValues(messageBuilder.build(), kfcCallback);
		} catch (RpcException e){
			// 这里不需要记录metric了，已经在 biliLoadBalance里面记录过了
			resultFuture.complete(null);
		} catch (Exception e){
			throw e;
		}
	}

	@Override
	public String createCacheKey(Object... inputs) {
		throw new UnsupportedOperationException("kfc does not create cache");
	}

	@Override
	public void close0() throws Exception {
		if (rpcClient!=null){
			rpcClient.stop();
		}

		// Null out sideMetricsWrapper to prevent memory leak
		sideMetricsWrapper = null;
	}

	public final class MultiKeyCallback extends KfcCallback {
        private String[] originKey;

        public MultiKeyCallback(CompletableFuture<Collection<RowData>> resultFuture,
                                boolean isMultiKeyJoin, String[] originKey, boolean rawInput) {
            super(resultFuture,isMultiKeyJoin,rawInput);
            this.originKey = originKey;
        }

        @Override
        public final void success(KfcMessage.KFCResponse response) {
            sideMetricsWrapper.sideJoinSuccess();
            Map<String, KfcMessage.KFCResponse.Result> resultMap = response.getResultsMap();

            if(resultMap==null || resultMap.size() == 0){
                resultFuture.complete(null);
            }else {
                sideMetricsWrapper.rtQuerySide(startTime);

                GenericRowData rowData = new GenericRowData(2);
                rowData.setField(KfcRowDataLookupFunction.this.KeyIdx,
                        StringData.fromString(String.join(KfcRowDataLookupFunction.this.delimitKey, originKey)));
                fillinResponseValue(resultMap, rowData);

                resultFuture.complete(Collections.singleton(rowData));
            }
        }

        @Override
        public final Integer fillinResponseValue(Map<String, KfcMessage.KFCResponse.Result> resultMap,
                                                    GenericRowData rowData) {
            String[] queryKey = originKey;
            if (rawInput) {
                byte[][] resultList = new byte[queryKey.length][];
                for (int i = 0; i < queryKey.length; i++) {
                    resultList[i] = parseRawValue(resultMap.get(queryKey[i]));
                }
                rowData.setField(KfcRowDataLookupFunction.this.ValueIdx, new GenericArrayData(resultList));
                return queryKey.length;
            } else {
                StringData[] resultList = new StringData[queryKey.length];
                for (int i = 0; i < queryKey.length; i++) {
                    resultList[i] = StringData.fromString(parseValue(resultMap.get(queryKey[i])));
                }
                rowData.setField(KfcRowDataLookupFunction.this.ValueIdx, new GenericArrayData(resultList) );
                return queryKey.length;
            }
        }
    }

    public final class SingleKeyCallback extends KfcCallback {
        private String originKey;

        public SingleKeyCallback(CompletableFuture<Collection<RowData>> resultFuture,
                                boolean isMultiKeyJoin, String originKey, boolean rawInput) {
            super(resultFuture,isMultiKeyJoin,rawInput);
            this.originKey = originKey;
        }

        @Override
        public final void success(KfcMessage.KFCResponse response) {
            sideMetricsWrapper.sideJoinSuccess();
            Map<String, KfcMessage.KFCResponse.Result> resultMap = response.getResultsMap();

            if(resultMap==null || resultMap.size() == 0){
                resultFuture.complete(null);
            }else {
                sideMetricsWrapper.rtQuerySide(startTime);

                GenericRowData rowData = new GenericRowData(2);
                rowData.setField(KfcRowDataLookupFunction.this.KeyIdx,
                        StringData.fromString(originKey));
                fillinResponseValue(resultMap, rowData);

                resultFuture.complete(Collections.singleton(rowData));
            }
        }

        @Override
        public final Integer fillinResponseValue(Map<String, KfcMessage.KFCResponse.Result> resultMap,
                                                 GenericRowData rowData) {
            if (rawInput) {
                    byte[] result = parseRawValue(resultMap.get(originKey));
                    rowData.setField(KfcRowDataLookupFunction.this.ValueIdx, result);
                } else {
                    String result = parseValue(resultMap.get(originKey));
                    rowData.setField(KfcRowDataLookupFunction.this.ValueIdx, StringData.fromString(result));
                }
				return 1;
        }
    }

	public abstract class KfcCallback implements RpcCallback<KfcMessage.KFCResponse> {

		protected CompletableFuture<Collection<RowData>> resultFuture;

//		private Object originKey;

        protected Long startTime = System.nanoTime();

        protected boolean isMultiKeyJoin;

        protected boolean rawInput;

		public KfcCallback( CompletableFuture<Collection<RowData>> resultFuture,
							boolean isMultiKeyJoin, boolean rawInput){


			this.resultFuture = resultFuture;
			this.isMultiKeyJoin = isMultiKeyJoin;
			this.rawInput = rawInput;
		}

		@Override
		public abstract void success(KfcMessage.KFCResponse response);

		@Override
		public void fail(Throwable e) {
			sideMetricsWrapper.rpcCallbackFailure();
			resultFuture.complete(null);
		}

		public abstract Integer fillinResponseValue(Map<String, KfcMessage.KFCResponse.Result> resultMap,
                                           GenericRowData rowData);

		//parse
		public String parseValue( KfcMessage.KFCResponse.Result result) {
			if (result == null){
				return "";
			}
			return result.getResultCase() != KfcMessage.KFCResponse.Result.ResultCase.VALUE ?
					"":result.getValue().toString(forName("UTF-8"));
		}

		public byte[] parseRawValue( KfcMessage.KFCResponse.Result result) {
			if (result == null || result.getResultCase() != KfcMessage.KFCResponse.Result.ResultCase.VALUE){
				return new byte[0];
			}

			byte[] convertedValue = new byte[result.getValue().size()];
			byte[] returnedValue = result.getValue().toByteArray();
			for (int i = 0; i<returnedValue.length;i++) {
				convertedValue[i] = returnedValue[i];
			}
			return convertedValue;
		}
	}
}
