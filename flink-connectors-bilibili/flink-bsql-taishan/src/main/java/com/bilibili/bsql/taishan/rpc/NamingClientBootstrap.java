package com.bilibili.bsql.taishan.rpc;


import pleiades.venus.naming.client.NamingClient;
import pleiades.venus.naming.discovery.DiscoveryNamingClient;
import pleiades.venus.naming.discovery.event.DiscoveryEventPublisher;
import pleiades.venus.naming.discovery.transport.DiscoveryTransportFactory;

/**
 * @author zhuzhengjun
 * @date 2020/10/30 10:37 下午
 */
public class NamingClientBootstrap {

    //注意！！！！！
    //namingClient 必须全局单例，否则会导致discovery服务端出现大量连接
    private final NamingClient                    namingClient;

    private volatile boolean                      hasStart = false;

    private static volatile NamingClientBootstrap bootstrap;

    public static NamingClientBootstrap getInstance() {
        if (bootstrap == null) {
            synchronized (NamingClientBootstrap.class) {
                if (bootstrap == null) {
                    bootstrap = new NamingClientBootstrap();
                }
            }
        }
        return bootstrap;
    }

    private NamingClientBootstrap() {
        DiscoveryEventPublisher eventPublisher = new DiscoveryEventPublisher();
        namingClient = new DiscoveryNamingClient(new DiscoveryTransportFactory(eventPublisher),
                eventPublisher);
        namingClient.start();
    }

    /**
     * start naming client
     * Note: this operation is blocking
     * and must invoke once
     */
    public void start() {
        //start
        if (!hasStart) {
            synchronized (NamingClientBootstrap.class) {
                if (!hasStart) {
                    namingClient.start();
                    hasStart = true;
                }
            }
        }
    }

    public synchronized void close() {
        if (hasStart) {
            synchronized (NamingClientBootstrap.class) {
                if (hasStart) {
                    namingClient.close();
                    hasStart = false;
                }
            }
        }
    }

    public NamingClient getNamingClient() {
        return namingClient;
    }
}
