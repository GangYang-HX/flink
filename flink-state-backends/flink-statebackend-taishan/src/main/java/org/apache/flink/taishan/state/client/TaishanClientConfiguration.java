package org.apache.flink.taishan.state.client;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.taishan.state.configuration.TaishanOptions;

import java.io.Serializable;

/**
 * @author Dove
 * @Date 2022/8/10 2:37 下午
 */
public class TaishanClientConfiguration implements Serializable {
	private static final long serialVersionUID = 1L;
	private final long scanSleepTimeBase;
	private String metaAddresses;
	private int clientTimeout;
	private int backupRequestThreadPoolSize;

	public static TaishanClientConfiguration loadClientConfiguration(ReadableConfig config) {
		String metaAddresses = config.get(TaishanOptions.TAISHAN_META_ADDRESSES);
		int clientTimeout = config.get(TaishanOptions.TAISHAN_CLIENT_TIMEOUT);
		int backupRequestThreadPoolSize = config.get(TaishanOptions.TAISHAN_BACKUP_REQUEST_THREAD_POOL_SIZE);
		long scanSleepTimeBase = config.get(TaishanOptions.TAISHAN_SCAN_SLEEP_TIME_BASE);

		return new TaishanClientConfiguration(metaAddresses, clientTimeout, backupRequestThreadPoolSize, scanSleepTimeBase);
	}

	public TaishanClientConfiguration(String metaAddresses, int clientTimeout, int backupRequestThreadPoolSize, long scanSleepTimeBase) {
		this.metaAddresses = metaAddresses;
		this.clientTimeout = clientTimeout;
		this.backupRequestThreadPoolSize = backupRequestThreadPoolSize;
		this.scanSleepTimeBase = scanSleepTimeBase;
	}

	public String getMetaAddresses() {
		return metaAddresses;
	}

	public int getClientTimeout() {
		return clientTimeout;
	}

	public int getBackupRequestThreadPoolSize() {
		return backupRequestThreadPoolSize;
	}

	public long getScanSleepTimeBase() {
		return scanSleepTimeBase;
	}
}
