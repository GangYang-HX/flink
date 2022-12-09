package org.apache.flink.taishan.state.bloomfilter;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.taishan.state.TaishanMetric;
import org.apache.flink.taishan.state.client.TaishanClientWrapper;
import org.apache.flink.taishan.state.serialize.TaishanSerializedCompositeKeyBuilder;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.taishan.state.bloomfilter.BloomFilterUtils.optimalNumOfBits;
import static org.apache.flink.taishan.state.bloomfilter.BloomFilterUtils.optimalNumOfHashFunctions;

public class TaishanBloomFilterClient implements BloomFilterClient {
	private static final Logger LOG = LoggerFactory.getLogger(TaishanBloomFilterClient.class);

	private final List<BloomFilter> bloomFilterList;
	private final int startKeyGroup;
	private final int endKeyGroup;
	private final String stateName;
	private final int keyGroupPrefixBytes;
	private final TaishanClientWrapper taishanClientWrapper;
	private TaishanMetric taishanMetric;

	public TaishanBloomFilterClient(String stateName, KeyGroupRange range, int keyGroupPrefixBytes, TaishanClientWrapper taishanClientWrapper) {
		this.stateName = stateName;
		this.bloomFilterList = new ArrayList<>();
		this.startKeyGroup = range.getStartKeyGroup();
		this.endKeyGroup = range.getEndKeyGroup();
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.taishanClientWrapper = taishanClientWrapper;
	}

	@Override
	public void snapshotFilter() {
		for (int keyGroupId = startKeyGroup; keyGroupId <= endKeyGroup; keyGroupId++) {
			BloomFilter bloomFilter = bloomFilterList.get(keyGroupId - startKeyGroup);
			TaishanSerializedCompositeKeyBuilder<String> keyBuilder =
				new TaishanSerializedCompositeKeyBuilder<>(
					stateName,
					StringSerializer.INSTANCE,
					keyGroupPrefixBytes,
					32);
			keyBuilder.setKey(stateName, keyGroupId);
			byte[] rawKeyBytes = keyBuilder.buildCompositeKeyNamespace(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
			byte[] bloomFilterBytes = serialize(bloomFilter);
			taishanClientWrapper.put(keyGroupId, rawKeyBytes, bloomFilterBytes, 0);
		}
	}

	@Override
	public void restore() {
		boolean isRestore = false;
		for (int keyGroupId = startKeyGroup; keyGroupId <= endKeyGroup; keyGroupId++) {
			TaishanSerializedCompositeKeyBuilder<String> keyBuilder =
				new TaishanSerializedCompositeKeyBuilder<>(
					stateName,
					StringSerializer.INSTANCE,
					keyGroupPrefixBytes,
					32);
			keyBuilder.setKey(stateName, keyGroupId);
			byte[] rawKeyBytes = keyBuilder.buildCompositeKeyNamespace(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);
			if (taishanClientWrapper != null) {
				byte[] rawValues = taishanClientWrapper.get(keyGroupId, rawKeyBytes);
				BloomFilter value = deserialize(rawValues);
				bloomFilterList.add(value);
				isRestore = rawValues != null;
			} else {
				BloomFilter value = createEmptyBloomFilter();
				bloomFilterList.add(value);
			}
		}
		if (isRestore) {
			LOG.info("Restore bloom filter for key group range: {}-{}", startKeyGroup, endKeyGroup);
		} else {
			LOG.info("Initializing bloom filter for key group range: {}-{}", startKeyGroup, endKeyGroup);
		}
	}

	@Override
	public void add(int keyGroupId, byte[] rawKeyBytes, int ttlTime) {
		BloomFilter bloomFilter = bloomFilterList.get(keyGroupId - startKeyGroup);
		Key bloomFilterKey = new Key(rawKeyBytes);
		if (!bloomFilter.membershipTest(bloomFilterKey)) {
			bloomFilter.add(bloomFilterKey);
		}
	}

	@Override
	public boolean mightContains(int keyGroupId, byte[] rawKeyBytes) {
		BloomFilter bloomFilter = bloomFilterList.get(keyGroupId - startKeyGroup);
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

	private BloomFilter deserialize(byte[] bytes) {
		final DataInputBuffer in = new DataInputBuffer();
		try {
			BloomFilter bloomFilter = createEmptyBloomFilter();
			if (bytes != null) {
				in.reset(bytes, bytes.length);
				bloomFilter.readFields(in);
			}
			return bloomFilter;
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error happened when deserializing bloom filter", e);
		}
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

	private BloomFilter createEmptyBloomFilter() {
		int bitSize = optimalNumOfBits(50000, 0.1);
		int numberOfHash = optimalNumOfHashFunctions(50000, bitSize);
		return new BloomFilter(bitSize, numberOfHash, Hash.MURMUR_HASH);
	}
}
