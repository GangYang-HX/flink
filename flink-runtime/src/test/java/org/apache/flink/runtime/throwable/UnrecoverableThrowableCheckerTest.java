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

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test {@link UnrecoverableThrowableChecker}.
 */
public class UnrecoverableThrowableCheckerTest extends TestLogger {

	@Test
	public void testCheck() {
		Throwable recoverableCause = new Exception();
		Throwable unrecoverableCause = new Exception();

		// mock rules
		Rule ruleA = mock(Rule.class);
		Rule ruleB = mock(Rule.class);
		when(ruleA.match(recoverableCause)).thenReturn(false);
		when(ruleB.match(recoverableCause)).thenReturn(false);
		when(ruleA.match(unrecoverableCause)).thenReturn(false);
		when(ruleB.match(unrecoverableCause)).thenReturn(true);

		List<Rule> rules = Arrays.asList(ruleA, ruleB);
		UnrecoverableThrowableChecker.setRules(rules);

		// null throwable
		assertFalse(UnrecoverableThrowableChecker.check(null));

		// recoverable throwable
		assertFalse(UnrecoverableThrowableChecker.check(recoverableCause));

		// unrecoverable throwable
		assertTrue(UnrecoverableThrowableChecker.check(unrecoverableCause));
	}
}
