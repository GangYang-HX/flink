package org.apache.flink.taishan.state;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Implementation of a lexicographically ordered set of byte-arrays, based on a {@link TreeSet}.
 */
public class TreeOrderedSetCache implements TaishanCachingPriorityQueueSet.OrderedByteArraySetCache {

	/** Maximum capacity. */
	private final int maxSize;

	@Nonnull
	private final TreeSet<byte[]> treeSet;

	TreeOrderedSetCache(int maxSize) {
		this.maxSize = maxSize;
		this.treeSet = new TreeSet<>(LEXICOGRAPHIC_BYTE_COMPARATOR);
	}

	@Override
	public int size() {
		return treeSet.size();
	}

	@Override
	public int maxSize() {
		return maxSize;
	}

	@Override
	public boolean isEmpty() {
		return treeSet.isEmpty();
	}

	@Override
	public boolean isFull() {
		return treeSet.size() >= maxSize;
	}

	@Override
	public boolean add(@Nonnull byte[] toAdd) {
		return treeSet.add(toAdd);
	}

	@Override
	public boolean remove(@Nonnull byte[] toRemove) {
		return treeSet.remove(toRemove);
	}

	@Nullable
	@Override
	public byte[] peekFirst() {
		return !isEmpty() ? treeSet.first() : null;
	}

	@Nullable
	@Override
	public byte[] peekLast() {
		return !isEmpty() ? treeSet.last() : null;
	}

	@Nullable
	@Override
	public byte[] pollFirst() {
		return !isEmpty() ? treeSet.pollFirst() : null;
	}

	@Nullable
	@Override
	public byte[] pollLast() {
		return !isEmpty() ? treeSet.pollLast() : null;
	}

	@Override
	public void bulkLoadFromOrderedIterator(@Nonnull Iterator<byte[]> orderedIterator) {
		treeSet.clear();
		for (int i = maxSize; --i >= 0 && orderedIterator.hasNext(); ) {
			treeSet.add(orderedIterator.next());
		}
	}
}
