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

import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test {@link IllegalStateExceptionRule}.
 */
public class IllegalStateExceptionRuleTest extends TestLogger {

	@Test
	public void testMatch() {
		IllegalStateExceptionRule illegalStateExceptionRule = new IllegalStateExceptionRule();

		// direct recoverable throwable
		assertFalse(illegalStateExceptionRule.match(new Exception()));
		assertFalse(illegalStateExceptionRule.match(new IllegalStateException()));
		assertFalse(illegalStateExceptionRule.match(new IllegalStateException("Out of range", null)));

		// nested recoverable throwable
		assertFalse(illegalStateExceptionRule.match(new Exception(new IllegalStateException())));
		assertFalse(illegalStateExceptionRule.match(new Exception(new IllegalStateException("Out of range", null))));

		// direct unrecoverable throwable
		assertTrue(illegalStateExceptionRule.match(new IllegalStateException(
			"The library registration references a different set of library BLOBs than " +
				"previous registrations for this job", null)));

		// nested unrecoverable throwable
		assertTrue(illegalStateExceptionRule.match(new Exception(new IllegalStateException(
			"The library registration references a different set of library BLOBs than " +
				"previous registrations for this job", null))));
	}
}
