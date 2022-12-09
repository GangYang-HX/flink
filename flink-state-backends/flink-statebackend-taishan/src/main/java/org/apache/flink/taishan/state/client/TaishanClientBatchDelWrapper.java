package org.apache.flink.taishan.state.client;

import io.grpc.CallOptions;
import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.cache.ByteArrayWrapper;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Dove
 * @Date 2022/7/7 2:32 下午
 */
public class TaishanClientBatchDelWrapper extends TaishanClientBatchBaseWrapper {

	private final Set<ByteArrayWrapper> recordBatch;
	private long currentBatchSize;

	public TaishanClientBatchDelWrapper(TaishanClientWrapper taishanClientWrapper, TaishanMetric taishanMetric) {
		super(taishanClientWrapper, taishanMetric);
		currentBatchSize = 0;
		recordBatch = new HashSet<>();
	}

	public void remove(
		int keyGroupId,
		ByteArrayWrapper key) {
		if (currentKeyGroupId != keyGroupId) {
			flush();
			currentKeyGroupId = keyGroupId;
		}

		recordBatch.add(key);
		currentBatchSize += key.array().length;

		flushIfNeeded();
	}

	private void flushIfNeeded() {
		boolean needFlush = recordBatch.size() == capacity || (batchSize > 0 && currentBatchSize >= batchSize);
		if (needFlush) {
			flush();
		}
	}

	@Override
	public void flush() {
		if (recordBatch.size() == 0) {
			return;
		}
		long start = taishanMetric.updateCatchingRate();
		taishanClientWrapper.batchDelOnShard(CallOptions.DEFAULT, currentKeyGroupId, recordBatch.stream().map(ByteArrayWrapper::array).collect(Collectors.toList()));
		taishanMetric.takeDeleteBatchMetrics(start, recordBatch);
		lastFlushTime = System.currentTimeMillis();

		clearAll();
	}

	public void clearAll() {
		recordBatch.clear();
		currentBatchSize = 0;
	}
}
