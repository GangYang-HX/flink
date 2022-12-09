/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.LambdaUtil;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

/**
 * Helpers for {@link StateObject} related code.
 */
public class StateUtil {

	private static final Logger LOG = LoggerFactory.getLogger(StateUtil.class);

	private StateUtil() {
		throw new AssertionError();
	}

	/**
	 * Returns the size of a state object
	 *
	 * @param handle The handle to the retrieved state
	 */
	public static long getStateSize(StateObject handle) {
		return handle == null ? 0 : handle.getStateSize();
	}

	/**
	 * Iterates through the passed state handles and calls discardState() on each handle that is not null. All
	 * occurring exceptions are suppressed and collected until the iteration is over and emitted as a single exception.
	 *
	 * @param handlesToDiscard State handles to discard. Passed iterable is allowed to deliver null values.
	 * @throws Exception exception that is a collection of all suppressed exceptions that were caught during iteration
	 */
	public static void bestEffortDiscardAllStateObjects(
			Iterable<? extends StateObject> handlesToDiscard) throws Exception {
		LambdaUtil.applyToAllWhileSuppressingExceptionsAndDoLog(handlesToDiscard, StateObject::discardStateWithRetry, LOG);
	}

	/**
	 * Discards the given state future by first trying to cancel it. If this is not possible, then
	 * the state object contained in the future is calculated and afterwards discarded.
	 *
	 * @param stateFuture to be discarded
	 * @throws Exception if the discard operation failed
	 */
	public static void discardStateFuture(Future<? extends StateObject> stateFuture) throws Exception {
		if (null != stateFuture) {
			if (!stateFuture.cancel(true)) {

				try {
					// We attempt to get a result, in case the future completed before cancellation.
					if (stateFuture instanceof RunnableFuture<?> && !stateFuture.isDone()) {
						((RunnableFuture<?>) stateFuture).run();
					}
					StateObject stateObject = stateFuture.get();
					if (null != stateObject) {
						stateObject.discardState();
					}

				} catch (CancellationException | ExecutionException ex) {
					LOG.debug("Cancelled execution of snapshot future runnable. Cancellation produced the following " +
						"exception, which is expected an can be ignored.", ex);
				}
			}
		}
	}

	public static boolean hasKeyedState(OperatorState operatorState) {
		Preconditions.checkState(operatorState != null);

		for (OperatorSubtaskState subtaskState : operatorState.getStates()) {
			if (subtaskState.getManagedKeyedState().hasState() ||
					subtaskState.getRawKeyedState().hasState()) {
				return true;
			}
		}

		return false;
	}

	public static OperatorState generateOperatorStateWithNewMaxParallelism(OperatorState operatorState,
																		   int maxParallelism) {
		OperatorState newOperatorState = new OperatorState(operatorState.getOperatorID(),
			operatorState.getParallelism(), maxParallelism);

		for (Map.Entry<Integer, OperatorSubtaskState> operatorSubtaskState :
			operatorState.getSubtaskStates().entrySet()) {
			newOperatorState.putState(operatorSubtaskState.getKey(), operatorSubtaskState.getValue());
		}

		newOperatorState.setCoordinatorState(operatorState.getCoordinatorState());
		return newOperatorState;
	}

	public static void updateCheckpointMaxParallelismIfNeeded(CompletedCheckpoint savepoint,
															  Map<JobVertexID, ExecutionJobVertex> tasks) {
		for (ExecutionJobVertex task : tasks.values()) {

			for (OperatorIDPair operatorIDPair : task.getOperatorIDs()) {
				OperatorID operatorID = operatorIDPair.getUserDefinedOperatorID().isPresent() ?
					operatorIDPair.getGeneratedOperatorID() : operatorIDPair.getGeneratedOperatorID();
				Map<OperatorID, OperatorState> operatorID2OperatorState = savepoint.getOperatorStates();
				OperatorState operatorState = operatorID2OperatorState.get(operatorID);
				if (operatorState == null) {
					continue;
				}
				if (!hasKeyedState(operatorState) &&
					operatorState.getMaxParallelism() < task.getParallelism()) {
					int newMaxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(task.getParallelism());
					operatorID2OperatorState.put(operatorID,
						generateOperatorStateWithNewMaxParallelism(operatorState, newMaxParallelism));
					LOG.info("Update task: {} ({}) max parallelism from {} to {}. ", task.getName(), operatorID,
						operatorState.getMaxParallelism(), newMaxParallelism);
				}
				// reset execution job vertex max parallelism to avoid the max parallelism check failed.
				task.updateMaxParallelism(operatorID2OperatorState.get(operatorID).getMaxParallelism());
			}

		}

	}
}
