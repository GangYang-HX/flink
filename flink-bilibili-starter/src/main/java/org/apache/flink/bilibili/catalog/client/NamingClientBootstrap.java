/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bilibili.catalog.client;

import pleiades.venus.naming.client.NamingClient;
import pleiades.venus.naming.discovery.DiscoveryNamingClient;
import pleiades.venus.naming.discovery.event.DiscoveryEventPublisher;
import pleiades.venus.naming.discovery.transport.DiscoveryTransportFactory;

public class NamingClientBootstrap {

	//注意！！！！！
	//namingClient 必须全局单例，否则会导致discovery服务端出现大量连接
	private final NamingClient namingClient;

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
		namingClient = new DiscoveryNamingClient(new DiscoveryTransportFactory(eventPublisher), eventPublisher);
		namingClient.start();
	}

	public NamingClient getNamingClient() {
		return namingClient;
	}
}
