package org.apache.flink.taishan.state.client;

import com.bilibili.taishan.model.Record;
import com.bilibili.taishan.model.ScanResp;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.taishan.state.TaishanMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author Dove
 * @Date 2022/7/6 2:30 下午
 */
public class TaishanClientIteratorWrapper {

	private final Logger LOG = LoggerFactory.getLogger(TaishanClientIteratorWrapper.class);

	private final TaishanClientWrapper taishanClientWrapper;
	private final int scanLimit;
	private final byte[] startRangeKey;
	private final TaishanMetric taishanMetric;
	private boolean useKeyPrefix = false;
	private byte[] keyPrefix;

	@Nullable
	private KeyGroupRange keyGroupRange;
	private int keyGroupId;
	private List<Record> records;
	private Record currentRecord;
	private ScanResp scanResp;
	private byte[] currentRangeKey;

	public TaishanClientIteratorWrapper(
		TaishanClientWrapper taishanClientWrapper,
		int keyGroupId,
		byte[] startRangeKey,
		int limit,
		TaishanMetric taishanMetric) {
		this(taishanClientWrapper, keyGroupId, startRangeKey, startRangeKey, limit, taishanMetric, false);
	}

	public TaishanClientIteratorWrapper(
		TaishanClientWrapper taishanClientWrapper,
		int keyGroupId,
		byte[] keyPrefix,
		byte[] startRangeKey,
		int limit,
		TaishanMetric taishanMetric,
		boolean useKeyPrefix) {
		this(taishanClientWrapper, startRangeKey, limit, taishanMetric);
		this.keyPrefix = keyPrefix;
		this.keyGroupId = keyGroupId;
		this.useKeyPrefix = useKeyPrefix;

		scanOnce();
	}

	public TaishanClientIteratorWrapper(
		TaishanClientWrapper taishanClientWrapper,
		KeyGroupRange keyGroupRange,
		byte[] startRangeKey,
		int limit,
		TaishanMetric taishanMetric) {
		this(taishanClientWrapper, startRangeKey, limit, taishanMetric);
		this.keyGroupRange = keyGroupRange;

		scanOnce();
	}

	private TaishanClientIteratorWrapper(
		TaishanClientWrapper taishanClientWrapper,
		byte[] startRangeKey,
		int limit,
		TaishanMetric taishanMetric) {
		this.taishanClientWrapper = taishanClientWrapper;
		this.startRangeKey = startRangeKey;
		this.scanLimit = limit;
		this.taishanMetric = taishanMetric;
		checkState(scanLimit > 1, "The limit size must be greater than 1.");
	}

	private void scanOnce() {
		byte[] rangeKey = currentRangeKey == null ? startRangeKey : currentRangeKey;
		long start = taishanMetric.updateCatchingRate();
		if (useKeyPrefix) {
			this.scanResp = taishanClientWrapper.scan(keyGroupId, keyPrefix, rangeKey, scanLimit);
		} else {
			this.scanResp = taishanClientWrapper.scan(keyGroupId, rangeKey, scanLimit);
		}
		taishanMetric.takeSeekMetrics(start);
		this.records = scanResp.getRecords();
		checkChangeKeyGroupId();
	}

	public boolean hasNext() {
		return currentKeyGroupIdHasNext() || hasNextKeyGroupId();
	}

	public void next() {
		if (records.size() > 0) {
			currentRecord = records.remove(0);
			currentRangeKey = currentRecord.getKey();
			return;
		}
		if (scanResp.isHasNext()) {
			currentRangeKey = scanResp.getNextKey();
			scanOnce();
			if (records.size() > 0) {
				currentRecord = records.remove(0);
				currentRangeKey = currentRecord.getKey();
				return;
			}
		}
		LOG.warn("scan record empty.");
	}

	public byte[] key() {
		return currentRecord.getKey();
	}

	public byte[] value() {
		return currentRecord.getValue();
	}

	private boolean currentKeyGroupIdHasNext() {
		return records.size() > 0 || scanResp.isHasNext();
	}

	private boolean hasNextKeyGroupId() {
		while (keyGroupRange != null && keyGroupId < keyGroupRange.getEndKeyGroup()) {
			keyGroupId++;
			currentRangeKey = null;
			scanOnce();
			if (hasNext()) {
				return true;
			}
		}
		return false;
	}

	private void checkChangeKeyGroupId() {
		if (currentKeyGroupIdHasNext()) {
			return;
		}
		hasNextKeyGroupId();
	}
}
