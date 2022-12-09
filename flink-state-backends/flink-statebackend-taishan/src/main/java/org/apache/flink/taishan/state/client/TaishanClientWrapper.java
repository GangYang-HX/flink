package org.apache.flink.taishan.state.client;

import com.bilibili.taishan.TaishanClient;
import com.bilibili.taishan.exception.TaishanException;
import com.bilibili.taishan.model.Record;
import com.bilibili.taishan.model.ScanParam;
import com.bilibili.taishan.model.ScanResp;
import io.grpc.CallOptions;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.flink.taishan.state.cache.OffHeapUtils.OFF_HEAP_KEY_PREFIX_LENGTH;

public class TaishanClientWrapper implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanClientWrapper.class);

	private static final int DEFAULT_TTL = 60 * 60 * 30;
	private static final int DEFAULT_RETRY_TIMES = 3;

	private final long scanSleepTimeBase;
	private final TaishanClient taishanCLient;
	private final String taishanTableName;

	public TaishanClientWrapper(TaishanClient taishanClient, String taishanTableName, long scanSleepTimeBase) {
		this.taishanCLient = taishanClient;
		this.taishanTableName = taishanTableName;
		this.scanSleepTimeBase = scanSleepTimeBase;
	}

	public void put(int keyGroupId, byte[] rangeKey, byte[] value, int ttl) {
		put(keyGroupId, rangeKey, value, ttl, DEFAULT_RETRY_TIMES);
	}

	private void put(int keyGroupId, byte[] rangeKey, byte[] value, int ttl, int retryTimes) {
		try {
			taishanCLient.put(CallOptions.DEFAULT, keyGroupId, rangeKey, value, ttl);
		} catch (TaishanException e) {
			logRetryMessage(e, retryTimes, "put");
			// todo code: TIMEOUT[1004], message: get leader replica timeout
			// todo code: TIMEOUT[1004], message: deadline exceeded after 0.998913136s. [remote_addr=/172.23.34.12:7300]
			// todo code: TIMEOUT[1004], message: ClientCall started after deadline exceeded: -0.001719190s from now
			if (retryTimes > 0) {
				put(keyGroupId, rangeKey, value, ttl, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while adding data to Taishan", e);
			}
		}
	}

	public byte[] get(int keyGroupId, byte[] rangeKey) {
		return get(keyGroupId, rangeKey, DEFAULT_RETRY_TIMES);
	}

	private byte[] get(int keyGroupId, byte[] rangeKey, int retryTimes) {
		byte[] valueBytes = null;
		try {
			valueBytes = taishanCLient.get(CallOptions.DEFAULT, keyGroupId, rangeKey);
		} catch (TaishanException e) {
			if (e.getCode() == 404) {
				// ignore error: KEY_NOT_FOUND[404], message: KeyNotFoundError
			} else if (retryTimes > 0) {
				logRetryMessage(e, retryTimes, "get");
				valueBytes = get(keyGroupId, rangeKey, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while retrieving data from Taishan. ", e);
			}
		}
		return valueBytes;
	}

	public void del(int keyGroupId, byte[] rangeKey) {
		del(keyGroupId, rangeKey, DEFAULT_RETRY_TIMES);
	}

	private void del(int keyGroupId, byte[] rangeKey, int retryTimes) {
		try {
			taishanCLient.del(CallOptions.DEFAULT, keyGroupId, rangeKey);
		} catch (TaishanException e) {
			logRetryMessage(e, retryTimes, "del");
			if (retryTimes > 0) {
				del(keyGroupId, rangeKey, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while delete data from Taishan. ", e);
			}
		}
	}

	public ScanResp scan(int keyGroupId, byte[] rangeKey, int limit) {
		return scan(keyGroupId, rangeKey, limit, DEFAULT_RETRY_TIMES);
	}

	private ScanResp scan(int keyGroupId, byte[] rangeKey, int limit, int retryTimes) {
		ScanParam scanParam = ScanParam.builder()
			.startKey(rangeKey)
			.preFetch(true)
			.limit(limit)
			.build();
		ScanResp scanResp = null;
		try {
			scanResp = taishanCLient.scan(CallOptions.DEFAULT, keyGroupId, scanParam);
		} catch (TaishanException e) {
			logRetryMessage(e, retryTimes, "scan1");
			if (retryTimes > 0) {
				sleep(scanSleepTimeBase, retryTimes);
				scan(keyGroupId, rangeKey, limit, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while scan data from Taishan. ", e);
			}
		}
		return scanResp;
	}

	public ScanResp scan(int keyGroupId, byte[] keyPrefix, byte[] rangeKey, int limit) {
		return scan(keyGroupId, keyPrefix, rangeKey, limit, DEFAULT_RETRY_TIMES);
	}

	private ScanResp scan(int keyGroupId, byte[] keyPrefix, byte[] rangeKey, int limit, int retryTimes) {
		ScanParam scanParam = ScanParam.builder()
			.startKey(rangeKey)
			.keyPrefix(keyPrefix)
			.preFetch(true)
			.limit(limit)
			.build();
		ScanResp scanResp = null;
		try {
			scanResp = taishanCLient.scan(CallOptions.DEFAULT, keyGroupId, scanParam);
		} catch (TaishanException e) {
			logRetryMessage(e, retryTimes, "scan2");
			if (retryTimes > 0) {
				sleep(scanSleepTimeBase, retryTimes);
				scanResp = scan(keyGroupId, keyPrefix, rangeKey, limit, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while scan data from Taishan. ", e);
			}
		}
		return scanResp;
	}

	public void batchPutOnShard(CallOptions callOptions, int hashCode, List<Record> records) {
		batchPutOnShard(callOptions, hashCode, records, DEFAULT_RETRY_TIMES);
	}

	private void batchPutOnShard(CallOptions callOptions, int hashCode, List<Record> records, int retryTimes) {
		try {
			taishanCLient.batchPutOnShard(callOptions, hashCode, records);
		} catch (TaishanException e) {
			logRetryMessage(e, retryTimes, "batchPutOnShard");
			if (retryTimes > 0) {
				batchPutOnShard(callOptions, hashCode, records, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while batchPut data to Taishan", e);
			}
		}
	}

	public void batchDelOnShard(CallOptions callOptions, int hashCode, List<byte[]> records) {
		batchDelOnShard(callOptions, hashCode, records, DEFAULT_RETRY_TIMES);
	}

	private void batchDelOnShard(CallOptions callOptions, int hashCode, List<byte[]> records, int retryTimes) {
		try {
			taishanCLient.batchDelOnShard(callOptions, hashCode, records);
		} catch (TaishanException e) {
			logRetryMessage(e, retryTimes, "batchDelOnShard");
			if (retryTimes > 0) {
				batchDelOnShard(callOptions, hashCode, records, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while batchDel data to Taishan", e);
			}
		}
	}

	public String createCheckpoint(int keyGroupId, String checkpointId) {
		return createCheckpoint(keyGroupId, checkpointId, DEFAULT_RETRY_TIMES);
	}

	private String createCheckpoint(int keyGroupId, String checkpointId, int retryTimes) {
		try {
			return taishanCLient.createCheckpoint(CallOptions.DEFAULT, keyGroupId, DEFAULT_TTL, checkpointId);
		} catch (TaishanException e) {
			logRetryMessage(e, retryTimes, "createCheckpoint", checkpointId, keyGroupId);
			if (retryTimes > 0) {
				return createCheckpoint(keyGroupId, checkpointId, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while create checkpoint from Taishan. ", e);
			}
		}
	}

	public void dropCheckpoint(int keyGroupId, String checkpointId) {
		dropCheckpoint(keyGroupId, checkpointId, DEFAULT_RETRY_TIMES);
	}

	private void dropCheckpoint(int keyGroupId, String checkpointId, int retryTimes) {
		try {
			taishanCLient.destroyCheckpoint(CallOptions.DEFAULT, keyGroupId, checkpointId);
		} catch (TaishanException e) {
			if (retryTimes > 0) {
				dropCheckpoint(keyGroupId, checkpointId, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while destroy checkpoint from Taishan. ", e);
			}
		}
	}

	public void switchCheckpoint(int keyGroupId, String checkpointId) {
		switchCheckpoint(keyGroupId, checkpointId, DEFAULT_RETRY_TIMES);
	}

	private void switchCheckpoint(int keyGroupId, String checkpointId, int retryTimes) {
		try {
			taishanCLient.switchCheckpoint(CallOptions.DEFAULT, keyGroupId, checkpointId);
		} catch (TaishanException e) {
			logRetryMessage(e, retryTimes, "switchCheckpoint", checkpointId, keyGroupId);
			if (retryTimes > 0) {
				switchCheckpoint(keyGroupId, checkpointId, --retryTimes);
			} else {
				throw new FlinkRuntimeException("Error while switch checkpoint from Taishan. ", e);
			}
		}
	}

	private void logRetryMessage(TaishanException e, int retryTimes, String methodName) {
		LOG.warn("TaishanException was obtained. Remaining retry times:{}, methodName:{}, taishanTableName:{}, message:{}", retryTimes, methodName, taishanTableName, e.getMessage());
	}

	private void logRetryMessage(TaishanException e, int retryTimes, String methodName, String checkpointId, int keyGroupId) {
		LOG.warn("TaishanException was obtained. Remaining retry times:{}, methodName:{}, taishanTableName:{}, message:{}" +
				", checkpointId:{}, keyGroupId:{}.",
			retryTimes, methodName, taishanTableName, e.getMessage(),
			checkpointId, keyGroupId);
	}

	public byte[] getOffHeapKeyPre() {
		return taishanTableName.substring(taishanTableName.length() - OFF_HEAP_KEY_PREFIX_LENGTH).getBytes(StandardCharsets.UTF_8);
	}

	private void sleep(long millisBase, int retryTimes) {
		int n = Math.abs(DEFAULT_RETRY_TIMES - retryTimes);
		long millis = millisBase * (1 << n);
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
		}
	}

	@Override
	public void close() {
		if (taishanCLient != null) {
			taishanCLient.close();
		}
	}
}
