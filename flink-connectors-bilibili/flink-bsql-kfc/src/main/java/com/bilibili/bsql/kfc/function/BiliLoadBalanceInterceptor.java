/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package com.bilibili.bsql.kfc.function;

import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.brpc.ChannelInfo;
import com.baidu.brpc.client.channel.BrpcChannel;
import com.baidu.brpc.exceptions.RpcException;
import com.baidu.brpc.interceptor.InterceptorChain;
import com.baidu.brpc.interceptor.LoadBalanceInterceptor;
import com.baidu.brpc.protocol.Request;
import com.baidu.brpc.protocol.Response;
import com.bilibili.bsql.common.metrics.SideMetricsWrapper;

/**
 *
 * @author zhouhuidong
 * 在brpc loadBalance上增加监控
 * @version $Id: BiliLoadBalanceInterceptor.java, v 0.1 2021-12-16 下午10:05 zhouhuidong Exp $$
 */
public class BiliLoadBalanceInterceptor extends LoadBalanceInterceptor {

	private final static Logger LOG = LoggerFactory.getLogger(BiliLoadBalanceInterceptor.class);

	protected SideMetricsWrapper sideMetricsWrapper;

	BiliLoadBalanceInterceptor(SideMetricsWrapper sideMetricsWrapper){
		this.sideMetricsWrapper = sideMetricsWrapper;
	}

	@Override
	public void aroundProcess(Request request, Response response, InterceptorChain chain) throws Exception {
		RpcException exception = null;
		int currentTryTimes = 0;
		int maxTryTimes = rpcClient.getRpcClientOptions().getMaxTryTimes();
		while (currentTryTimes < maxTryTimes) {
			try {
				// if it is a retry request, add the last selected instance to request,
				// so that load balance strategy can exclude the selected instance.
				// if it is the initial request, not init HashSet, so it is more fast.
				// therefore, it need LoadBalanceStrategy to judge if selectInstances is null.
				if (currentTryTimes > 0) {
					if (request.getChannel() != null) {
						if (request.getSelectedInstances() == null) {
							request.setSelectedInstances(new HashSet<BrpcChannel>(maxTryTimes - 1));
						}
						BrpcChannel lastInstance = ChannelInfo
							.getClientChannelInfo(request.getChannel()).getChannelGroup();
						request.getSelectedInstances().add(lastInstance);
					}
				}
				invokeRpc(request, response);
				break;
			} catch (RpcException ex) {
				sideMetricsWrapper.rpcCallbackFailure(ex.getCode());
				exception = ex;
				if (exception.getCode() == RpcException.INTERCEPT_EXCEPTION) {
					break;
				}
			} finally {
				currentTryTimes++;
			}
		}
		if (response.getResult() == null && response.getRpcFuture() == null) {

			if (exception == null) {
				exception = new RpcException(RpcException.UNKNOWN_EXCEPTION, "unknown error");
			}
			response.setException(exception);
		}
	}
}
