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

package org.apache.flink.runtime.throwable;

import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link NoResourceAvailableExceptionRule}.
 */
public class NoResourceAvailableExceptionRuleTest extends TestLogger {

	@Test
	public void testMatch() {
		NoResourceAvailableExceptionRule noResourceAvailableExceptionRule = new NoResourceAvailableExceptionRule();

		// direct recoverable throwable
		assertFalse(noResourceAvailableExceptionRule.match(new Exception()));
		assertFalse(noResourceAvailableExceptionRule.match(new NoResourceAvailableException()));
		assertFalse(noResourceAvailableExceptionRule.match(new NoResourceAvailableException("no resources")));

		// nested recoverable throwable
		assertFalse(noResourceAvailableExceptionRule.match(new Exception(new NoResourceAvailableException())));
		assertFalse(noResourceAvailableExceptionRule.match(new Exception(
			new NoResourceAvailableException("no resources"))));

		// direct unrecoverable throwable
		assertTrue(noResourceAvailableExceptionRule.match(new NoResourceAvailableException("Could not allocate the " +
			"required slot within slot request timeout. Please make sure that the cluster has enough resources")));

		// nested unrecoverable throwable
		assertTrue(noResourceAvailableExceptionRule.match(new Exception(
			new NoResourceAvailableException("Could not allocate the required slot within slot request timeout. " +
				"Please make sure that the cluster has enough resources"))));
	}
}
