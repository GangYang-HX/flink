package org.apache.flink.taishan.state.bloomfilter;

import org.apache.flink.util.TestLogger;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.apache.flink.taishan.state.bloomfilter.BloomFilterUtils.optimalNumOfBits;
import static org.apache.flink.taishan.state.bloomfilter.BloomFilterUtils.optimalNumOfHashFunctions;
import static org.junit.Assert.*;

public class TestCountingBloomFilter extends TestLogger {
	private int slotSize = 100000;

	private int numInsertions = 100000;

	@Test
	public void testBloomFilterSerialization() throws Exception {
		final Random rnd = new Random();
		final DataOutputBuffer out = new DataOutputBuffer();
		final DataInputBuffer in = new DataInputBuffer();

		int bitSize = optimalNumOfBits(numInsertions, 0.1);
		int numberOfHash = optimalNumOfHashFunctions(numInsertions, bitSize);
		CountingBloomFilter filter = new CountingBloomFilter(bitSize, numberOfHash, Hash.MURMUR_HASH);
		ImmutableList.Builder<Integer> blist = ImmutableList.builder();
		for (int i = 0; i < slotSize; i++) {
			blist.add(rnd.nextInt(numInsertions * 2));
		}

		ImmutableList<Integer> list = blist.build();

		// mark bits for later check
		for (Integer slot : list) {
			filter.add(new Key(String.valueOf(slot).getBytes()));
		}

		filter.write(out);
		in.reset(out.getData(), out.getLength());
		CountingBloomFilter tempFilter = new CountingBloomFilter(bitSize, numberOfHash, Hash.MURMUR_HASH);
		tempFilter.readFields(in);

		String filterString = filter.toString();
		String tempFilterString = tempFilter.toString();
		assertEquals(filterString, tempFilterString);

		for (Integer slot : list) {
			assertTrue("read/write mask check filter error on " + slot,
				tempFilter.membershipTest(new Key(String.valueOf(slot).getBytes())));
		}

		for (Integer slot : list) {
			int randomInt = rnd.nextInt();
			if (randomInt % 2 == 0) {
				filter.delete(new Key(String.valueOf(slot).getBytes()));
				tempFilter.delete(new Key(String.valueOf(slot).getBytes()));
			}
		}

		filterString = filter.toString();
		tempFilterString = tempFilter.toString();
		assertEquals(filterString, tempFilterString);

		final DataOutputBuffer newOut = new DataOutputBuffer();
		final DataOutputBuffer tempOut = new DataOutputBuffer();
		filter.write(newOut);
		tempFilter.write(tempOut);

		assertEquals(newOut.getData().length, tempOut.getData().length);
		assertTrue(Arrays.equals(newOut.getData(), tempOut.getData()));
	}

	@Test
	public void testBloomFilterAddAndDelete() throws Exception {
		int slotSize = 10000000;
		final Random rnd = new Random();
		final DataOutputBuffer out = new DataOutputBuffer();
		final DataInputBuffer in = new DataInputBuffer();

		int bitSize = optimalNumOfBits(numInsertions, 0.1);
		int numberOfHash = optimalNumOfHashFunctions(numInsertions, bitSize);
		CountingBloomFilter filter = new CountingBloomFilter(bitSize, numberOfHash, Hash.MURMUR_HASH);
		Set<Integer> clist = new HashSet<>();
		Set<Integer> dList = new HashSet<>();
		for (int i = 0; i < slotSize; i++) {
			clist.add(rnd.nextInt(numInsertions * 2));
		}

		// mark bits for later check
		for (Integer slot : clist) {
			filter.add(new Key(String.valueOf(slot).getBytes()));
			int randomInt = rnd.nextInt();
			if (randomInt % 2 == 0) {
				if (filter.membershipTest(new Key(String.valueOf(slot).getBytes()))) {
					filter.delete(new Key(String.valueOf(slot).getBytes()));
				}
				if (filter.membershipTest(new Key(String.valueOf(slot).getBytes()))) {
					filter.delete(new Key(String.valueOf(slot).getBytes()));
				}
				dList.add(slot);
			}
		}

		for (Integer slot : clist) {
			if (!dList.contains(slot)) {
				assertTrue("read/write mask check filter error on " + slot,
					filter.membershipTest(new Key(String.valueOf(slot).getBytes())));
			}
		}
	}
}
