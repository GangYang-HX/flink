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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.partition.listener.BacklogEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for the {@link MinBacklogLoadBasedStrategy}
 */
public class MinBacklogLoadBasedStrategyTest {

	private final SerializationDelegate<IOReadableWritable> serializationDelegate =
		new SerializationDelegate<>(null);

	private static final int COUNTS = 10_000_000;

	@Test
	public void testGetNextChannelToSendTo(){
		MinBacklogLoadBasedStrategy strategy = new MinBacklogLoadBasedStrategy();
		int next = 0;
		int nextByGet = 0;

		for (int i = 0; i < COUNTS; i++) {
			next = (next + 1) % 9;
			nextByGet = strategy.nextChannel(nextByGet, 9);
			assertEquals(nextByGet, next);
		}
	}

	@Test
	public void testSetup() {
		MinBacklogLoadBasedStrategy minBacklogLoadBasedStrategy = new MinBacklogLoadBasedStrategy();
		final int numberOfChannels = 10;
		minBacklogLoadBasedStrategy.setup(numberOfChannels);
		int initialChannel = minBacklogLoadBasedStrategy.select(serializationDelegate);

		assertTrue(0 <= initialChannel);
		assertTrue(numberOfChannels > initialChannel);

		for (int i = 1; i <= numberOfChannels * 2; i++) {
			assertSelectedChannel(minBacklogLoadBasedStrategy, i % numberOfChannels);
		}
	}

	@Test
	public void testSelectInterval() {
		MinBacklogLoadBasedStrategy minBacklogLoadBasedStrategy = new MinBacklogLoadBasedStrategy();
		Random random = new Random();
		final int numberOfChannels = 10;
		minBacklogLoadBasedStrategy.setup(numberOfChannels);

		for (int i = 0; i < 10; i++) {
			int backlog = random.nextInt(2);
			BacklogEvent event = new BacklogEvent(i, backlog);
			minBacklogLoadBasedStrategy.update(event);
		}

		for (int i = 0; i < 10; i++) {
			int selectChannel = minBacklogLoadBasedStrategy.select(serializationDelegate);
			assertTrue(0 <= selectChannel);
			assertTrue(numberOfChannels > selectChannel);
		}
	}

	@Test
	public void testSelectWithUpdateInterval() {
		MinBacklogLoadBasedStrategy minBacklogLoadBasedStrategy = new MinBacklogLoadBasedStrategy(100);
		Random random = new Random();
		final int numberOfChannels = 10;
		minBacklogLoadBasedStrategy.setup(numberOfChannels);

		for (int i = 0; i < 1000; i++) {
			int index = random.nextInt(numberOfChannels);
			int backlog = random.nextInt(2);
			try {
				Thread.sleep(random.nextInt(5));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			BacklogEvent event = new BacklogEvent(index, backlog);
			minBacklogLoadBasedStrategy.update(event);
		}

		for (int i = 0; i < 10; i++) {
			int selectChannel = minBacklogLoadBasedStrategy.select(serializationDelegate);
			assertTrue(0 <= selectChannel);
			assertTrue(numberOfChannels > selectChannel);
		}
	}

	private void assertSelectedChannel(MinBacklogLoadBasedStrategy minBacklogLoadBasedStrategy, int expectedChannel) {
		int actualResult = minBacklogLoadBasedStrategy.select(serializationDelegate);
		assertEquals(expectedChannel, actualResult);
	}
}
