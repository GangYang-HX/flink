package org.apache.flink.taishan.state.bloomfilter;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;
import org.apache.flink.taishan.state.serialize.TaishanSerializedCompositeKeyBuilder;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SegmentedBloomFilterClient implements BloomFilterClient {

	private static final Logger LOG = LoggerFactory.getLogger(SegmentedBloomFilterClient.class);
	private final List<SegmentedBloomFilter> bloomFilterList;
	private final int numberOfSegments;
	private final int startKeyGroup;
	private final int endKeyGroup;
	private final String stateName;
	private final int keyGroupPrefixBytes;
	private final TaishanClientWrapper taishanClientWrapper;
	private TaishanMetric taishanMetric;

	public SegmentedBloomFilterClient(String stateName, KeyGroupRange range, int keyGroupPrefixBytes, int numberOfSegments, TaishanClientWrapper taishanClientWrapper) {
		this.stateName = stateName;
		this.startKeyGroup = range.getStartKeyGroup();
		this.endKeyGroup = range.getEndKeyGroup();
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		Preconditions.checkArgument(numberOfSegments > 1, "numberOfSegments must be greater than 1, but is:" + numberOfSegments);
		this.numberOfSegments = numberOfSegments;
		this.bloomFilterList = new ArrayList<>();
		this.taishanClientWrapper = taishanClientWrapper;
	}

	@Override
	public void snapshotFilter() {
		for (int keyGroupId = startKeyGroup; keyGroupId <= endKeyGroup; keyGroupId++) {
			SegmentedBloomFilter bloomFilter = bloomFilterList.get(keyGroupId - startKeyGroup);
			TaishanSerializedCompositeKeyBuilder<String> keyBuilder =
				new TaishanSerializedCompositeKeyBuilder<>(
					stateName,
					StringSerializer.INSTANCE,
					keyGroupPrefixBytes,
					32);
			for (int i = 0; i < bloomFilter.numberOfSegments; i++) {
				keyBuilder.setKey(stateName + "-" + i, keyGroupId);
				byte[] rawKeyBytes = keyBuilder.buildCompositeKeyNamespace(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
				byte[] bloomFilterBytes = serialize(bloomFilter.countingBloomFilters[i]);
				taishanClientWrapper.put(keyGroupId, rawKeyBytes, bloomFilterBytes, 0);
			}
		}
	}


	@Override
	public void restore() {
		boolean restoreFlag = false;
		for (int keyGroupId = startKeyGroup; keyGroupId <= endKeyGroup; keyGroupId++) {
			TaishanSerializedCompositeKeyBuilder<String> keyBuilder =
				new TaishanSerializedCompositeKeyBuilder<>(
					stateName,
					StringSerializer.INSTANCE,
					keyGroupPrefixBytes,
					32);
			SegmentedBloomFilter bloomFilter = new SegmentedBloomFilter(numberOfSegments);
			for (int i = 0; i < bloomFilter.numberOfSegments; i++) {
				keyBuilder.setKey(stateName + "-" + i, keyGroupId);
				byte[] rawKeyBytes = keyBuilder.buildCompositeKeyNamespace(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
				byte[] rawValues = taishanClientWrapper.get(keyGroupId, rawKeyBytes);
				if (rawValues != null) {
					// restore SegmentedBloomFilter
					restoreFlag = true;
					final DataInputBuffer in = new DataInputBuffer();
					in.reset(rawValues, rawValues.length);
					BloomFilter countingBloomFilter = bloomFilter.countingBloomFilters[i];
					try {
						countingBloomFilter.readFields(in);
					} catch (IOException e) {
						throw new FlinkRuntimeException("Error happened when deserializing bloom filter", e);
					}
				} else {
					// init SegmentedBloomFilter
				}
			}
			bloomFilterList.add(bloomFilter);
		}
		LOG.info("Initializing/Restore segmented bloom filter for key group range: {}-{}, is restore:{}", startKeyGroup, endKeyGroup, restoreFlag);
	}

	@Override
	public void add(int keyGroupId, byte[] rawKeyBytes, int ttlTime) {
		SegmentedBloomFilter bloomFilter = bloomFilterList.get(keyGroupId - startKeyGroup);
		Key bloomFilterKey = new Key(rawKeyBytes);
		if (!bloomFilter.membershipTest(bloomFilterKey)) {
			bloomFilter.add(bloomFilterKey);
		}
	}

	@Override
	public boolean mightContains(int keyGroupId, byte[] rawKeyBytes) {
		SegmentedBloomFilter bloomFilter = bloomFilterList.get(keyGroupId - startKeyGroup);
		Key bloomFilterKey = new Key(rawKeyBytes);
		boolean contains = bloomFilter.membershipTest(bloomFilterKey);
		if (!contains) {
			taishanMetric.takeBloomFilterNull();
		}
		return contains;
	}

	@Override
	public void delete(int keyGroupId, byte[] rawKeyBytes) {
	}

	@Override
	public void initTaishanMetric(TaishanMetric taishanMetric) {
		this.taishanMetric = taishanMetric;
	}

	private byte[] serialize(BloomFilter filter) {
		final DataOutputBuffer out = new DataOutputBuffer();
		try {
			filter.write(out);
			return out.getData();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error happened when serializing bloom filter", e);
		}
	}
}
