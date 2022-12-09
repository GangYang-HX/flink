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

import java.util.Arrays;
import java.util.List;

/**
 * Define a rule for java.lang.IllegalStateException,
 * and the exception matching logic follows {@link ExceptionNameAndMessagesMatchingRule}.
 */
public class IllegalStateExceptionRule extends ExceptionNameAndMessagesMatchingRule {

	private static final String FULL_QUALIFIED_EXCEPTION_NAME = "java.lang.IllegalStateException";

	private static final List<String> EXCEPTION_MESSAGES = Arrays.asList(
		"The library registration references a different set of library BLOBs than previous registrations for this job");

	@Override
	String getExceptionName() {
		return FULL_QUALIFIED_EXCEPTION_NAME;
	}

	@Override
	List<String> getExceptionMessages() {
		return EXCEPTION_MESSAGES;
	}
}
