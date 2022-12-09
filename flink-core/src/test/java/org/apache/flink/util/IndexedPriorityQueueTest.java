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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link IndexedPriorityQueue}
 */
public class IndexedPriorityQueueTest {

	@Test
	public void isEmpty() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(2);
		assertTrue(priorityQueue.isEmpty());

		priorityQueue.insert(0, 1);
		assertFalse(priorityQueue.isEmpty());
	}

	@Test
	public void testContainsIndex() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(2);
		assertFalse(priorityQueue.containsIndex(0));

		priorityQueue.insert(0, 1);
		assertTrue(priorityQueue.containsIndex(0));
	}

	@Test
	public void testInsert() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(4);
		priorityQueue.insert(0, 3);
		priorityQueue.insert(1, 2);
		priorityQueue.insert(2, 1);
		priorityQueue.insert(3, 3);

		for (int i = 0; i < 4; i++) {
			assertTrue(priorityQueue.containsIndex(i));
		}
	}

	@Test
	public void testMinIndex() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(4);
		priorityQueue.insert(0, 3);
		priorityQueue.insert(1, 2);
		priorityQueue.insert(2, 1);
		priorityQueue.insert(3, 3);

		assertEquals(2, priorityQueue.minIndex());
	}

	@Test
	public void testMinValue() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(4);
		priorityQueue.insert(0, 3);
		priorityQueue.insert(1, 2);
		priorityQueue.insert(2, 1);
		priorityQueue.insert(3, 3);

		assertEquals(1, (int) priorityQueue.minValue());
	}

	@Test
	public void testDelMin() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(4);
		priorityQueue.insert(0, 0);
		priorityQueue.insert(1, 1);
		priorityQueue.insert(2, 2);
		priorityQueue.insert(3, 3);

		assertTrue(priorityQueue.containsIndex(0));
		priorityQueue.delMin();
		assertFalse(priorityQueue.containsIndex(0));
	}

	@Test
	public void testGet() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(4);
		for (int i = 0; i < 4; i++) {
			priorityQueue.insert(i, i);
		}

		for (int i = 0; i < 4; i++) {
			Integer value = priorityQueue.get(i);
			assertEquals((int) value, i);
		}
	}

	@Test
	public void changeKey() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(4);
		for (int i = 0; i < 4; i++) {
			priorityQueue.insert(i, i);
		}

		priorityQueue.changeKey(0, 8);
		assertNotEquals(0, (int) priorityQueue.get(0));
		assertEquals(8, (int) priorityQueue.get(0));
	}

	@Test
	public void delete() {
		IndexedPriorityQueue<Integer> priorityQueue = new IndexedPriorityQueue<>(4);
		for (int i = 0; i < 4; i++) {
			priorityQueue.insert(i, i);
		}

		for (int i = 0; i < 4; i++) {
			priorityQueue.delete(i);
			assertFalse(priorityQueue.containsIndex(i));
		}
	}
}
