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

package org.apache.flink.state.api.input.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.state.api.functions.RawKeyedStateReaderFunction;
import org.apache.flink.state.api.input.BinaryMultiStateKeyIterator;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A {@link RawStateReaderOperator} for executing a {@link RawKeyedStateReaderFunction}.
 *
 * @param <KEY> The key type read from the state backend.
 * @param <OUT> The output type of the function.
 */
@Internal
public class RawKeyedStateReaderOperator<KEY, OUT>
	extends RawStateReaderOperator<RawKeyedStateReaderFunction<KEY, OUT>, KEY, VoidNamespace, OUT> {

	private static final String USER_TIMERS_NAME = "user-timers";

	private transient Context context;

	public RawKeyedStateReaderOperator(RawKeyedStateReaderFunction<KEY, OUT> function, TypeInformation keyType) {
		super(function, keyType, VoidNamespaceSerializer.INSTANCE);
	}

	public RawKeyedStateReaderOperator(RawKeyedStateReaderFunction<KEY, OUT> function, TypeSerializer keyTypeSerializer) {
		super(function, keyTypeSerializer, VoidNamespaceSerializer.INSTANCE);
	}

	@Override
	public void open() throws Exception {
		super.open();

		InternalTimerService<VoidNamespace> timerService = getInternalTimerService(USER_TIMERS_NAME);
		context = new Context<>(getKeyedStateBackend(), timerService);
	}


	@Override
	public void processElement(KEY deserializedKey, byte[] key, VoidNamespace namespace, String cf, byte[] value, Collector<OUT> out) throws Exception {
		function.readKey(deserializedKey, key, cf, value, context, out);
	}

	@Override
	public Iterator<Tuple5<KEY, byte[], VoidNamespace, String, byte[]>> getKeysAndNamespacesAndColumnFamiliesAndValues(
			SavepointRuntimeContext ctx) throws Exception {
		ctx.disableStateRegistration();
		List<StateDescriptor<?, ?>> stateDescriptors = ctx.getStateDescriptors();
		Iterator<Tuple4<KEY, byte[], String, byte[]>> multiStateKeyIterator
			= new BinaryMultiStateKeyIterator(stateDescriptors, getKeyedStateBackend());
		return new VoidNamespaceDecoratedIterator(multiStateKeyIterator);
	}


	private static class Context<K> implements RawKeyedStateReaderFunction.Context {

		private static final String EVENT_TIMER_STATE = "event-time-timers";

		private static final String PROC_TIMER_STATE = "proc-time-timers";

		ListState<Long> eventTimers;

		ListState<Long> procTimers;

		private Context(KeyedStateBackend<K> keyedStateBackend, InternalTimerService<VoidNamespace> timerService) throws Exception {
			eventTimers = keyedStateBackend.getPartitionedState(
				USER_TIMERS_NAME,
				StringSerializer.INSTANCE,
				new ListStateDescriptor<>(EVENT_TIMER_STATE, Types.LONG));

			timerService.forEachEventTimeTimer((namespace, timer) -> {
				if (namespace.equals(VoidNamespace.INSTANCE)) {
					eventTimers.add(timer);
				}
			});

			procTimers = keyedStateBackend.getPartitionedState(
				USER_TIMERS_NAME,
				StringSerializer.INSTANCE,
				new ListStateDescriptor<>(PROC_TIMER_STATE, Types.LONG));

			timerService.forEachProcessingTimeTimer((namespace, timer) -> {
				if (namespace.equals(VoidNamespace.INSTANCE)) {
					procTimers.add(timer);
				}
			});
		}

		@Override
		public Set<Long> registeredEventTimeTimers() throws Exception {
			Iterable<Long> timers = eventTimers.get();
			if (timers == null) {
				return Collections.emptySet();
			}

			return StreamSupport
				.stream(timers.spliterator(), false)
				.collect(Collectors.toSet());
		}

		@Override
		public Set<Long> registeredProcessingTimeTimers() throws Exception {
			Iterable<Long> timers = procTimers.get();
			if (timers == null) {
				return Collections.emptySet();
			}

			return StreamSupport
				.stream(timers.spliterator(), false)
				.collect(Collectors.toSet());
		}
	}

	private static class VoidNamespaceDecoratedIterator<KEY> implements Iterator<Tuple5<KEY, byte[], VoidNamespace, String, byte[]>> {

		private final Iterator<Tuple4<KEY, byte[], String, byte[]>> iterator;

		private VoidNamespaceDecoratedIterator(Iterator<Tuple4<KEY, byte[], String, byte[]>> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Tuple5<KEY, byte[], VoidNamespace, String, byte[]> next() {
			Tuple4<KEY, byte[], String, byte[]> tuple4 = iterator.next();
			return Tuple5.of(tuple4.f0, tuple4.f1, VoidNamespace.INSTANCE, tuple4.f2, tuple4.f3);
		}

		@Override
		public void remove() {
			iterator.remove();
		}
	}
}
