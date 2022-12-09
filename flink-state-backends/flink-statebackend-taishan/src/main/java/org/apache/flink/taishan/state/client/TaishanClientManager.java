package org.apache.flink.taishan.state.client;


import com.bilibili.taishan.AccessOptions;
import com.bilibili.taishan.ClientManager;
import com.bilibili.taishan.MetaOptions;
import com.bilibili.taishan.exception.TaishanException;
import com.bilibili.taishan.prometheus.aspectj.MetricAspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;


public class TaishanClientManager {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanClientManager.class);

	public static synchronized ClientManager getInstance(TaishanClientConfiguration taishanClientConfiguration) {
		ClientManager clientManager = null;
		// disable taishan client metrics
		MetricAspect.setEnable(false);
		MetaOptions metaOptions = new MetaOptions();
		String metaAddress = taishanClientConfiguration.getMetaAddresses();
		String[] split = metaAddress.split(",");
		metaOptions.setAddrs(new HashSet<>(Arrays.asList(split)));
		metaOptions.setTimeoutMs(taishanClientConfiguration.getClientTimeout());
		try {
			clientManager = ClientManager.getClientManager(metaOptions);
			LOG.info("init taishan client manager finished with metaAddress:{}.", metaAddress);
		} catch (TaishanException e) {
			LOG.error("failed to init taishan client manager:{}", e.getMessage());
		}
		return clientManager;
	}

//	public static synchronized ClientManager getInstance(String metaAddress) {
//		if (clientManager == null) {
//			// disable taishan client metrics
//			MetricAspect.setEnable(false);
//			MetaOptions metaOptions = new MetaOptions();
//			String[] split = metaAddress.split(",");
//			metaOptions.setAddrs(new HashSet<>(Arrays.asList(split)));
//			try {
//				clientManager = ClientManager.getClientManager(metaOptions);
//				accessOptions = initOptions();
//				LOG.info("init taishan client manager finished with metaAddress:{}.",metaAddress);
//			} catch (TaishanException e) {
//				System.err.println("failed to init taishan client manager, " + e.getMessage());
//			}
//		}
//		return clientManager;
//	}

	public static AccessOptions getAccessOptions(TaishanClientConfiguration configuration) {
		return initOptions(configuration);
	}

	private static AccessOptions initOptions(TaishanClientConfiguration configuration) {
		AccessOptions accessOptions = new AccessOptions();
		int clientTimeout = configuration.getClientTimeout();
		int backupRequestThreadPoolSize = configuration.getBackupRequestThreadPoolSize();
		accessOptions.setTimeoutMs(clientTimeout);
		accessOptions.setBackupRequestThreadPoolSize(backupRequestThreadPoolSize);
		accessOptions.setReadPolicy(AccessOptions.ReadPolicy.PrimaryOnly);
		return accessOptions;
	}
}
