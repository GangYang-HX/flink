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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Define a rule that uses exception name and exception messages to match.
 *
 * An exception matches the rule needs to meet all of the following conditions:
 * 1) The exception name must be equal to the name defined in the rule.
 * 2) The exception message must contain at least one of the messages defined in the rule.
 *
 * Moreover, using String to describe the exception name for matching instead of Class,
 * because the dependency of the exception to be matched may not exist in the runtime module,
 * such as redis.clients.jedis.exceptions.JedisDataException.
 */
public abstract class ExceptionNameAndMessagesMatchingRule implements Rule {

	@Override
	public boolean match(Throwable cause) {
		Preconditions.checkNotNull(cause, "The exception to be matched should not be null");

		String exceptionName = getExceptionName();
		List<String> exceptionMessages = getExceptionMessages();

		Throwable t = cause;
		while (t != null) {
			String originalClassName;
			if (t instanceof SerializedThrowable) {
				originalClassName = ((SerializedThrowable) t).getOriginalErrorClassName();
			} else {
				originalClassName = t.getClass().getName();
			}
			if (!originalClassName.equals(exceptionName)) {
				t = t.getCause();
				continue;
			}

			if (t.getMessage() == null || t.getMessage().equals("")) {
				t = t.getCause();
				continue;
			}

			for (String message : exceptionMessages) {
				if (t.getMessage().contains(message)) {
					return true;
				}
			}

			t = t.getCause();
		}

		return false;
	}

	/**
	 * Return the exception name.
	 *
	 * @return the exception name.
	 */
	@Nonnull
	abstract String getExceptionName();

	/**
	 * Return the exception messages.
	 *
	 * @return the exception messages.
	 */
	@Nonnull
	abstract List<String> getExceptionMessages();
}
