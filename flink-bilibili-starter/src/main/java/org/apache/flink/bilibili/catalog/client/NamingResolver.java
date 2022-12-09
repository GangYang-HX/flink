package org.apache.flink.bilibili.catalog.client;

import org.apache.flink.bilibili.common.enums.AppIdEnum;
import pleiades.venus.naming.client.NamingClient;
import pleiades.venus.naming.client.Namings;

public class NamingResolver {

	//keeper的NamingResolver
	private static volatile pleiades.venus.naming.client.resolve.NamingResolver keeperResolver;
	//shield的NamingResolver
	private static volatile pleiades.venus.naming.client.resolve.NamingResolver shieldResolver;

	/**
	 * 获取和初始化keeper的NamingResolver
	 * @return
	 */
	public static pleiades.venus.naming.client.resolve.NamingResolver getKeeperResolver() {
		if (keeperResolver == null) {
			synchronized (NamingResolver.class) {
				if (keeperResolver == null) {
					NamingClient namingClient = NamingClientBootstrap.getInstance().getNamingClient();
					keeperResolver = namingClient.resolveForReady(AppIdEnum.KEEPER.getAppId(), Namings.Scheme.GRPC);
				}
			}
		}
		return keeperResolver;
	}

	/**
	 * 获取和初始化Shield的NamingResolver
	 * @return
	 */
	public static pleiades.venus.naming.client.resolve.NamingResolver getShieldResolver() {
		if (shieldResolver == null) {
			synchronized (NamingResolver.class) {
				if (shieldResolver == null) {
					NamingClient namingClient = NamingClientBootstrap.getInstance().getNamingClient();
					shieldResolver = namingClient.resolveForReady(AppIdEnum.SHIELD.getAppId(), Namings.Scheme.GRPC);
				}
			}
		}
		return shieldResolver;
	}
}
