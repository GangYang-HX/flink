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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.rpc.RpcService;

import java.lang.reflect.Constructor;

/**
 * {@link DispatcherFactory} which creates a {@link StandaloneDispatcher}.
 */
public enum SessionDispatcherFactory implements DispatcherFactory {
	INSTANCE;

	@Override
	public StandaloneDispatcher createDispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			DispatcherBootstrap dispatcherBootstrap,
			PartialDispatcherServicesWithJobGraphStore partialDispatcherServicesWithJobGraphStore) throws Exception {
		Configuration configuration = partialDispatcherServicesWithJobGraphStore.getConfiguration();
		Class<? extends StandaloneDispatcher> dispatcherClass = Class.forName(configuration.getString(CoreOptions.EXTEND_DISPATCHER_CLASSNAME)).asSubclass(StandaloneDispatcher.class);
		Constructor<? extends StandaloneDispatcher> constructor = dispatcherClass.getConstructor(RpcService.class, DispatcherId.class, DispatcherBootstrap.class, DispatcherServices.class);

		// create the default dispatcher
		return constructor.newInstance(
			rpcService,
			fencingToken,
			dispatcherBootstrap,
			DispatcherServices.from(partialDispatcherServicesWithJobGraphStore, DefaultJobManagerRunnerFactory.INSTANCE));
	}
}
