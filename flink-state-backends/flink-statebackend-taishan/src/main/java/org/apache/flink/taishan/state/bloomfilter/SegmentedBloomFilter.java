package org.apache.flink.taishan.state.bloomfilter;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.flink.taishan.state.bloomfilter.BloomFilterUtils.optimalNumOfBits;
import static org.apache.flink.taishan.state.bloomfilter.BloomFilterUtils.optimalNumOfHashFunctions;

public class SegmentedBloomFilter extends Filter {
	int numberOfSegments;
	public BloomFilter[] countingBloomFilters;

	public SegmentedBloomFilter(int numberOfSegments) {
		this.numberOfSegments = numberOfSegments;
		this.countingBloomFilters = new BloomFilter[numberOfSegments];
		for (int i = 0; i < numberOfSegments; i++) {
			this.countingBloomFilters[i] = createEmptyBloomFilter();
		}
	}

	public void add(Key key) {
		int bytesPerSegment = key.getBytes().length / numberOfSegments;
		for (int i = 0; i < numberOfSegments; i++) {
			BloomFilter bloomFilter = countingBloomFilters[i];
			int startIndex = i * numberOfSegments;
			int endIndex = Math.min(startIndex + bytesPerSegment, key.getBytes().length);
			byte[] keyBytes = ArrayUtils.subarray(key.getBytes(), startIndex, endIndex);
			Key bloomFilterKey = new Key(keyBytes);
			if (!bloomFilter.membershipTest(bloomFilterKey)) {
				bloomFilter.add(bloomFilterKey);
			}
		}
	}

	public boolean membershipTest(Key key) {
		int bytesPerSegment = key.getBytes().length / numberOfSegments;
		boolean contains = true;
		for (int i = 0; i < numberOfSegments; i++) {
			BloomFilter bloomFilter = countingBloomFilters[i];
			int startIndex = i * numberOfSegments;
			int endIndex = Math.min(startIndex + bytesPerSegment, key.getBytes().length);
			byte[] keyBytes = ArrayUtils.subarray(key.getBytes(), startIndex, endIndex);
			Key bloomFilterKey = new Key(keyBytes);
			if (!bloomFilter.membershipTest(bloomFilterKey)) {
				contains = false;
				break;
			}
		}
		return contains;
	}

	public void delete(Key key) {
	}

	public BloomFilter createEmptyBloomFilter() {
		int bitSize = optimalNumOfBits(10000, 0.1);
		int numberOfHash = optimalNumOfHashFunctions(10000, bitSize);
		return new BloomFilter(bitSize, numberOfHash, Hash.MURMUR_HASH);
	}

	public void not() {
		throw new UnsupportedOperationException("not() is undefined for " + this.getClass().getName());
	}

	public void or(Filter filter) {
		throw new UnsupportedOperationException("not() is undefined for " + this.getClass().getName());
	}

	public void xor(Filter filter) {
		throw new UnsupportedOperationException("xor() is undefined for " + this.getClass().getName());
	}

	public void and(Filter filter) {
		throw new UnsupportedOperationException("and() is undefined for " + this.getClass().getName());
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(numberOfSegments);
		for (int i = 0; i < numberOfSegments; i++) {
			countingBloomFilters[i].write(out);
		}

	}

	public void readFields(DataInput in) throws IOException {
		numberOfSegments = in.readInt();
		for (int i = 0; i < numberOfSegments; i++) {
			countingBloomFilters[i].readFields(in);
		}
	}
}
