package com.bilibili.bsql.taishan.format;

import avro.shaded.com.google.common.collect.Lists;
import com.bapis.infra.service.taishan.*;
import com.bilibili.bsql.taishan.rpc.NamingClientBootstrap;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pleiades.component.env.v1.Env;
import pleiades.component.rpc.client.BiliRpcClientCallInterceptor;
import pleiades.component.rpc.client.ChannelBuilder;
import pleiades.component.rpc.client.naming.RPCNamingClientNameResolverFactory;
import pleiades.component.stats.ModelRegister;
import pleiades.venus.breaker.BiliBreakerProperties;
import pleiades.venus.trace.TraceClient;
import pleiades.venus.trace.TraceProperties;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.bilibili.bsql.taishan.format.TaishanNamingResolver.TAISHAN_API_ID;

public class TaishanRPC {
    private final static Logger LOG = LoggerFactory.getLogger(TaishanRPC.class);
    public static final String SABER_ENGINE_API_ID = "bilibili.datacenter.saber-streamer.saber-engine";
    private final Channel channel;
    //全局唯一的，一个JVM只注册一次
    static {
        //这行代码必须有，自动生成trace id，taishan跟jaegertracing藕在一起了，必须要trace id
        TraceProperties traceProperties = new TraceProperties();
        TraceClient.init(SABER_ENGINE_API_ID, traceProperties);
        //这行代码必须有，初始化监控参数
        ModelRegister.register();
        LOG.info("static register finish ...");
    }

    public TaishanRPC(String proxyZone, String proxyCluster, int keepAliveTime, int keepAliveTimeout, int idleTimeout) {
        //重新加载Env，后面discovery的环境变量都来自这个类
        Properties properties = new Properties();
        properties.setProperty("app_id", SABER_ENGINE_API_ID);
        properties.setProperty("deploy_env", "prod");
        //properties.setProperty("deploy_env", "uat");// todo test code
        properties.setProperty("discovery_zone", proxyZone);
        Env.reload(properties);
		ManagedChannel managedChannel = buildChannel(proxyZone, proxyCluster, keepAliveTime, keepAliveTimeout,
			idleTimeout);
        //这行代码必须有，否则没有拦截器没有trace id
        BiliBreakerProperties prop = new BiliBreakerProperties();
        prop.setErrorRate(100);
        this.channel = ClientInterceptors.intercept(managedChannel,
                Lists.newArrayList(new BiliRpcClientCallInterceptor(prop)));
    }


    /**
     * 批量写入
     *
     * @param batchPutReq
     * @return
     */
    public BatchPutResp batchPut(BatchPutReq batchPutReq, int duration) {
        //采用阻塞的方式 超时时间2秒
        BatchPutResp reply = TaishanProxyGrpc.newBlockingStub(channel).withDeadlineAfter(duration, TimeUnit.SECONDS)
                .withWaitForReady()
                .batchPut(batchPutReq);
        return reply;
    }

    /**
     * 获取
     *
     * @param getReq
     * @return
     */
    public GetResp get(GetReq getReq) {
        GetResp reply = TaishanProxyGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                .withWaitForReady()
                .get(getReq);
        return reply;
    }


    /**
     * Async get
     *
     * @param getReq getReq
     * @return future
     */
    public ListenableFuture<GetResp> getAsync(GetReq getReq, long timeout) {
        return TaishanProxyGrpc.newFutureStub(channel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
                .withWaitForReady()
                .get(getReq);
    }

    /**
     * Async batch get
     *
     * @param batchGetReq batchGetReq
     * @return future
     */
    public ListenableFuture<BatchGetResp> batchGetAsync(BatchGetReq batchGetReq, long timeout) {
        return TaishanProxyGrpc.newFutureStub(channel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
                .withWaitForReady()
                .batchGet(batchGetReq);
    }

    private ManagedChannel buildChannel(String proxyZone, String proxyCluster, int keepAliveTime, int keepAliveTimeout,
										int idleTimeout) {
		return ChannelBuilder.forTarget(TAISHAN_API_ID)
			.disableRetry()
			.directExecutor()
			.defaultLoadBalancingPolicy("round_robin")
			.usePlaintext()
			.keepAliveTime(keepAliveTime, TimeUnit.SECONDS)
			.keepAliveTimeout(keepAliveTimeout, TimeUnit.MILLISECONDS)
			.idleTimeout(idleTimeout, TimeUnit.MINUTES)
			.nameResolverFactory(new RPCNamingClientNameResolverFactory(proxyZone, TaishanNamingResolver.getResolver(), proxyCluster))
			.build();
    }

    public void close() {
        NamingClientBootstrap.getInstance().close();
    }
}
