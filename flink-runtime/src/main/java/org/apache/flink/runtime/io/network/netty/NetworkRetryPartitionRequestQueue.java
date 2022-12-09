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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link PartitionRequestQueue} used when runtime network retry is enabled.
 */
public class NetworkRetryPartitionRequestQueue extends PartitionRequestQueue {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkRetryPartitionRequestQueue.class);

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		LOG.error("ChannelInactive called from NetworkRetryPartitionRequestQueue: " + ctx.name());

		if (!allReaders.isEmpty()) {
			for (NetworkSequenceViewReader reader : allReaders.values()) {
				reader.notifyConsumerUnavailable();
			}
			releaseAllResources();
		}
		ctx.fireChannelInactive();
	}

	/**
	 * When the connection between the client and the server is broken,
	 * the server will do the following things and wait for establishing a new connection:
	 * 1) Mark the PipelinedSubpartition unavailable to clear the incomplete records at producer
	 * and prevent data from being written to the buffer pool;
	 * 2) Mark the previous PipelinedSubpartitionView unavailable.
	 */
	@Override
	protected void handleException(Channel channel, Throwable cause) throws IOException {
		LOG.error("Encountered error while consuming partitions", cause);

		fatalError = true;

		if (!allReaders.isEmpty()) {
			for (NetworkSequenceViewReader reader : allReaders.values()) {
				reader.notifyConsumerUnavailable();
			}
			releaseAllResources();
		}

		if (channel.isActive()) {
			channel.writeAndFlush(new NettyMessage.ErrorResponse(cause)).addListener(ChannelFutureListener.CLOSE);
		}
	}
}
