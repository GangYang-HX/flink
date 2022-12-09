/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The {@code IndexedPriorityQueue} class represents an indexed priority queue of generic keys.
 * It supports the usual <em>insert</em> and <em>delete-the-minimum</em>
 * operations, along with <em>delete</em> and <em>change-the-key</em>
 * methods. In order to let the client refer to keys on the priority queue,
 * an integer between {@code 0} and {@code maxN - 1}
 * is associated with each key—the client uses this integer to specify
 * which key to delete or change.
 * It also supports methods for peeking at the minimum key,
 * testing if the priority queue is empty, and iterating through
 * the keys.
 * <p>
 * This implementation uses a binary heap along with an array to associate
 * keys with integers in the given range.
 * The <em>insert</em>, <em>delete-the-minimum</em>, <em>delete</em>,
 * <em>change-key</em> operations take &Theta;(log <em>n</em>) time in the worst case,
 * where <em>n</em> is the number of elements in the priority queue.
 * Construction takes time proportional to the specified capacity.
 * <p>
 *
 * @param <E> the generic type of key on this priority queue
 */
public class IndexedPriorityQueue<E extends Comparable<E>> implements Iterable<Integer> {

	private static final int NO_ELEMENTDATA = -1;

	/**
	 * Maximum number of elements on priority queue.
	 */
	private final int capacity;

	/**
	 * The number of elements on priority queue.
	 */
	private int size;

	/**
	 * The binary heap which store the index of items.
	 */
	private final int[] indexHeap;

	/**
	 * A array from the items index to its position in the binary heap,
	 * the value is the position of indexHeap.
	 */
	private final int[] inverseIndexHeap;

	private final E[] items;

	/**
	 * Initializes an empty indexed priority queue with indices between {@code 0}
	 * and {@code maxN - 1}.
	 *
	 * @param capacity the keys on this priority queue are index from {@code 0}
	 *                 {@code maxN - 1}
	 * @throws IllegalArgumentException if {@code maxN < 0}
	 */
	public IndexedPriorityQueue(int capacity) {
		Preconditions.checkArgument(capacity > 0, "capacity " + capacity + " must greater than 0");
		this.capacity = capacity;
		this.size = 0;
		this.items = (E[]) new Comparable[capacity + 1];
		this.indexHeap = new int[capacity + 1];
		this.inverseIndexHeap = new int[capacity + 1];
		for (int i = 0; i <= capacity; i++) {
			this.inverseIndexHeap[i] = NO_ELEMENTDATA;
		}
	}

	/**
	 * Returns true if this priority queue is empty.
	 *
	 * @return {@code true} if this priority queue is empty;
	 * {@code false} otherwise
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * Is {@code index} an index on this priority queue?
	 *
	 * @param index an index
	 * @return {@code true} if {@code index} is an index on this priority queue;
	 * {@code false} otherwise
	 */
	public boolean containsIndex(int index) {
		Preconditions.checkElementIndex(index, capacity);
		return inverseIndexHeap[index] != NO_ELEMENTDATA;
	}

	/**
	 * Returns the number of keys on this priority queue.
	 *
	 * @return the number of keys on this priority queue
	 */
	public int size() {
		return size;
	}

	/**
	 * Associates key with index {@code index}.
	 *
	 * @param index an index
	 * @param e     the key to associate with index {@code index}
	 * @throws IllegalArgumentException unless {@code 0 <= index < maxN}
	 * @throws IllegalArgumentException if there already is an item associated
	 *                                  with index {@code index}
	 */
	public void insert(int index, E e) {
		Preconditions.checkElementIndex(index, capacity);
		Preconditions.checkArgument(!containsIndex(index),
			"Index " + index + " is already in the priority queue");

		size++;
		inverseIndexHeap[index] = size;
		indexHeap[size] = index;
		items[index] = e;
		swim(size);
	}

	/**
	 * Returns an index associated with a minimum key.
	 *
	 * @return an index associated with a minimum key
	 * @throws NoSuchElementException if this priority queue is empty
	 */
	public int minIndex() {
		checkNotEmpty();
		return indexHeap[1];
	}

	private void checkNotEmpty() {
		if (size == 0) {
			throw new NoSuchElementException("Priority queue underflow");
		}
	}

	/**
	 * Returns a minimum value.
	 *
	 * @return a minimum value
	 * @throws NoSuchElementException if this priority queue is empty
	 */
	public E minValue() {
		checkNotEmpty();
		return items[indexHeap[1]];
	}

	/**
	 * Removes a minimum key and returns its associated index.
	 *
	 * @return an index associated with a minimum key
	 * @throws NoSuchElementException if this priority queue is empty
	 */
	public int delMin() {
		checkNotEmpty();
		int min = indexHeap[1];
		exch(1, size--);
		sink(1);
		assert min == indexHeap[size + 1];
		inverseIndexHeap[min] = NO_ELEMENTDATA;        // delete
		items[min] = null;    // to help with garbage collection
		indexHeap[size + 1] = NO_ELEMENTDATA;        // not needed
		return min;
	}

	/**
	 * Returns the key associated with index {@code index}.
	 *
	 * @param index the index of the key to return
	 * @return the key associated with index {@code index}
	 * @throws IllegalArgumentException unless {@code 0 <= index < maxN}
	 * @throws NoSuchElementException   no key is associated with index {@code index}
	 */
	public E get(int index) {
		Preconditions.checkElementIndex(index, capacity);
		if (containsIndex(index)) {
			return items[index];
		} else {
			throw new NoSuchElementException("Index " + index + " is not in the priority queue");
		}
	}

	/**
	 * Change the key associated with index {@code index} to the specified value.
	 *
	 * @param index the index of the key to change
	 * @param e     change the key associated with index {@code index} to this key
	 */
	public void changeKey(int index, E e) {
		Preconditions.checkElementIndex(index, capacity);
		Preconditions.checkArgument(containsIndex(index), "Index: " + index + " is not in the priority queue");
		if (items[index].equals(e)) {
			return;
		}

		items[index] = e;
		swim(inverseIndexHeap[index]);
		sink(inverseIndexHeap[index]);
	}

	/**
	 * Remove the key associated with index {@code index}.
	 *
	 * @param index the index of the key to remove
	 */
	public void delete(int index) {
		Preconditions.checkElementIndex(index, capacity);
		if (!containsIndex(index)) {
			return;
		}
		int temp = inverseIndexHeap[index];
		exch(temp, size--);
		swim(temp);
		sink(temp);
		items[index] = null;
		inverseIndexHeap[index] = NO_ELEMENTDATA;
	}


	/***************************************************************************
	 * General helper functions.
	 ***************************************************************************/
	private boolean greater(int i, int j) {
		return items[indexHeap[i]].compareTo(items[indexHeap[j]]) > 0;
	}

	private void exch(int i, int j) {
		int temp = indexHeap[i];
		indexHeap[i] = indexHeap[j];
		indexHeap[j] = temp;
		inverseIndexHeap[indexHeap[i]] = i;
		inverseIndexHeap[indexHeap[j]] = j;
	}


	/***************************************************************************
	 * Heap helper functions.
	 ***************************************************************************/
	private void swim(int k) {
		while (k > 1 && greater(k / 2, k)) {
			exch(k, k / 2);
			k = k / 2;
		}
	}

	private void sink(int k) {
		while (2 * k <= size) {
			int j = 2 * k;
			if (j < size && greater(j, j + 1)) {
				j++;
			}
			if (!greater(k, j)) {
				break;
			}
			exch(k, j);
			k = j;
		}
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append('[');
		for (int i = 0; ; i++) {
			b.append(items[i]);
			if (i == capacity - 1) {
				return b.append(']').toString();
			}
			b.append(", ");
		}
	}

	/**
	 * Returns an iterator that iterates over the keys on the
	 * priority queue in ascending order.
	 * The iterator doesn't implement {@code remove()} since it's optional.
	 *
	 * @return an iterator that iterates over the keys in ascending order
	 */
	public Iterator<Integer> iterator() {
		return new HeapIterator();
	}

	private class HeapIterator implements Iterator<Integer> {
		// create a new priority queue
		private final IndexedPriorityQueue<E> copy;

		// add all elements to copy of heap
		public HeapIterator() {
			copy = new IndexedPriorityQueue<E>(indexHeap.length - 1);
			for (int i = 1; i <= size; i++) {
				copy.insert(indexHeap[i], items[indexHeap[i]]);
			}
		}

		public boolean hasNext() {
			return !copy.isEmpty();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

		public Integer next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			return copy.delMin();
		}
	}
}
