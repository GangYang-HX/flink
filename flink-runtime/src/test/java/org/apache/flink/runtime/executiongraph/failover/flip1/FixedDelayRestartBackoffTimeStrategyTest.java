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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit tests for {@link FixedDelayRestartBackoffTimeStrategy}.
 */
public class FixedDelayRestartBackoffTimeStrategyTest extends TestLogger {

	private final Exception failure = new Exception();

	@Test
	public void testNumberOfRestarts() {
		final int numberOfRestarts = 3;

		final FixedDelayRestartBackoffTimeStrategy strategy =
				new FixedDelayRestartBackoffTimeStrategy(numberOfRestarts, 0L,
						FixedDelayRestartBackoffTimeStrategy.DISABLE_RESETTING_PERIOD);

		for (int restartsLeft = numberOfRestarts; restartsLeft > 0; --restartsLeft) {
			strategy.notifyFailure(failure);
			// two calls to 'canRestart()' to make sure this is not used to maintain the counter
			assertTrue(strategy.canRestart());
			assertTrue(strategy.canRestart());
		}

		strategy.notifyFailure(failure);
		assertFalse(strategy.canRestart());
	}

	@Test
	public void testBackoffTime() {
		final long backoffTimeMS = 10_000L;

		final FixedDelayRestartBackoffTimeStrategy restartStrategy =
				new FixedDelayRestartBackoffTimeStrategy(1, backoffTimeMS,
						FixedDelayRestartBackoffTimeStrategy.DISABLE_RESETTING_PERIOD);

		assertEquals(backoffTimeMS, restartStrategy.getBackoffTime());
	}

	@Test
	public void testResetAttemptsValue() throws InterruptedException {
		final int numberOfRestarts = 3;

		final FixedDelayRestartBackoffTimeStrategy strategy =
				new FixedDelayRestartBackoffTimeStrategy(numberOfRestarts, 0L, 500L);

		for (int restartsLeft = numberOfRestarts; restartsLeft > 0; --restartsLeft) {
			strategy.notifyFailure(failure);
			// two calls to 'canRestart()' to make sure this is not used to maintain the counter
			assertTrue(strategy.canRestart());
			assertTrue(strategy.canRestart());
		}
		// wait 600ms for resetting attempts value.
		Thread.sleep(600);
		for (int restartsLeft = numberOfRestarts; restartsLeft > 0; --restartsLeft) {
			strategy.notifyFailure(failure);
			assertTrue(strategy.canRestart());
			assertTrue(strategy.canRestart());
		}
		strategy.notifyFailure(failure);
		assertFalse(strategy.canRestart());

	}
}
