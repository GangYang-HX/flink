/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.throwable;

import org.apache.flink.annotation.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;

/**
 * Check whether an exception triggers the unrecoverable exception rules.
 * If so, the task should fail immediately, because it will not succeed even with retry.
 */
public class UnrecoverableThrowableChecker {

	/** Unrecoverable exception rules. */
	private final static List<Rule> unrecoverableExceptionRules = new ArrayList<>();

	/* Init rules. */
	static{
		unrecoverableExceptionRules.add(new BatchUpdateExceptionRule());
		unrecoverableExceptionRules.add(new NoResourceAvailableExceptionRule());
		unrecoverableExceptionRules.add(new JedisDataExceptionRule());
		unrecoverableExceptionRules.add(new IllegalStateExceptionRule());
		unrecoverableExceptionRules.add(new ClickHouseExceptionRule());
		unrecoverableExceptionRules.add(new BatchFlushExceptionRule());
	}

	/**
	 * Set new rules.
	 *
	 * @param rules the new rules to set.
	 */
	@VisibleForTesting
	static void setRules(List<Rule> rules) {
		unrecoverableExceptionRules.clear();
		unrecoverableExceptionRules.addAll(rules);
	}

	/**
	 * Return whether an exception triggers the unrecoverable exception rules.
	 *
	 * @param cause the exception to check.
	 * @return whether an exception triggers the unrecoverable exception rules.
	 */
	public static boolean check(Throwable cause) {
		if (cause == null) {
			return false;
		}

		for (Rule rule : unrecoverableExceptionRules) {
			if (rule.match(cause)) {
				return true;
			}
		}

		return false;
	}
}
