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

import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;


/**
 * {@link CreditBasedPartitionRequestClientHandler} used when runtime network retry is enabled.
 */

public class NetworkRetryCreditBasePartitionRequestClientHandler extends CreditBasedPartitionRequestClientHandler {
	private static final Logger LOG = LoggerFactory.getLogger(NetworkRetryCreditBasePartitionRequestClientHandler.class);

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (cause instanceof TransportException) {
			notifyAllChannelsOfErrorAndClose(cause);
		} else {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			// Improve on the connection reset by peer error message
			if (cause instanceof IOException && cause.getMessage().equals("Connection reset by peer")) {
				final TransportException tex = new RemoteTransportException(
						"Lost connection to task manager '" + remoteAddr + "'. " +
						"This indicates that the remote task manager was lost.", remoteAddr, cause);

				notifyAllChannelsOfErrorAndClose(tex);
			} else {
				// This indicates that the connection between the client and the server is broken.
				// Try to reconnect, and do the following things:
				// 1) Mark the RemoteInputChannel unavailable to clear the incomplete records at consumer;
				// 2) Remove the previous PartitionRequestClient;
				// 3) Establish a new connection with the server.

				LOG.error(
					String.format(
						"Connection problem with the server, try to reconnect. %s (connection to '%s')",
						cause.getMessage(),
						remoteAddr));

				List<RemoteInputChannel> reconnectChannels = new ArrayList();
				reconnectChannels.addAll(inputChannels.values());
				inputChannels.clear();

				LOG.info("Mark RemoteInputChannel unavailable.");
				reconnectChannels.forEach(RemoteInputChannel::markUnavailable);

				LOG.info("Remove the previous PartitionRequestClient.");
				removePrePartitionRequestClient(reconnectChannels);

				LOG.info("Try to reconnect, total channels num is {}.", reconnectChannels.size());
				for (RemoteInputChannel channel : reconnectChannels) {
					LOG.info("The channel to be reconnected is {}.", channel.getInputChannelId());
					channel.markReconnection();
				}
			}
		}
	}

	/**
	 * A channel binds a PartitionRequestClient and a CreditBasedPartitionRequestClientHandler.
	 * When the channel is broken, the PartitionRequestClient is also unavailable and can no longer
	 * send PartitionRequest through it.
	 * Therefore, to prevent the unavailable PartitionRequestClient from being used,
	 * we need to remove it before reconnecting.
	 */
	private void removePrePartitionRequestClient(List<RemoteInputChannel> reconnectChannels) {
		if (!reconnectChannels.isEmpty()) {
			//Sometimes multiple RemoteInputChannels share the same PartitionRequestClient,
			// we only need to remove it once.
			reconnectChannels.get(0).removePartitionRequestClient();
		}
	}
}
