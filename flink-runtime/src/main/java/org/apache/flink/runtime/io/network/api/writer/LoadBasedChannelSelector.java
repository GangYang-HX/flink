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

/**
 * This is the load based implementation of the {@link ChannelSelector} interface.
 *
 * @param <T> the type of record which is sent through the attached output gate
 */
public class LoadBasedChannelSelector<T extends IOReadableWritable> implements ChannelSelector<T> {

	private final LoadBasedStrategy loadBasedStrategy;

	public LoadBasedChannelSelector(LoadBasedStrategy loadBasedStrategy) {
		this.loadBasedStrategy = loadBasedStrategy;
	}

	@Override
	public void setup(int numberOfChannels) {
		loadBasedStrategy.setup(numberOfChannels);
	}

	@Override
	public int selectChannel(final T record) {
		return loadBasedStrategy.select(record);
	}

	@Override
	public boolean isBroadcast() {
		return false;
	}

	@Override
	public boolean isLoadBasedChannelSelectorEnabled() {
		return true;
	}

	@Override
	public String toString() {
		return "LOADBASED";
	}
}
