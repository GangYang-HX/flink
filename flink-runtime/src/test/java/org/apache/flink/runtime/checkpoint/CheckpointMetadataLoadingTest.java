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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.StringUtils;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A test that checks that checkpoint metadata loading works properly, including validation
 * of resumed state and dropped state.
 */
public class CheckpointMetadataLoadingTest {

	private final ClassLoader cl = getClass().getClassLoader();

	/**
	 * Tests correct savepoint loading.
	 */
	@Test
	public void testAllStateRestored() throws Exception {
		final JobID jobId = new JobID();
		final OperatorID operatorId = new OperatorID();
		final long checkpointId = Integer.MAX_VALUE + 123123L;
		final int parallelism = 128128;

		final CompletedCheckpointStorageLocation testSavepoint = createSavepointWithOperatorSubtaskState(checkpointId, operatorId, parallelism);
		final Map<JobVertexID, ExecutionJobVertex> tasks = createTasks(operatorId, parallelism, parallelism);

		final CompletedCheckpoint loaded = Checkpoints.loadAndValidateCheckpoint(jobId, tasks, testSavepoint, cl, false);

		assertEquals(jobId, loaded.getJobId());
		assertEquals(checkpointId, loaded.getCheckpointID());
	}

	/**
	 * Tests that savepoint loading fails when there is a max-parallelism mismatch.
	 */
	@Test
	public void testMaxParallelismMismatch() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;

		final CompletedCheckpointStorageLocation testSavepoint = createSavepointWithOperatorSubtaskState(242L, operatorId, parallelism);
		final Map<JobVertexID, ExecutionJobVertex> tasks = createTasks(operatorId, parallelism, parallelism + 1);

		try {
			Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false);
			fail("Did not throw expected Exception");
		} catch (RestoreCheckpointException expected) {
			assertTrue(findThrowableWithMessage(expected, "Max parallelism mismatch").isPresent());
		}
	}

	/**
	 * Tests that savepoint loading fails when the operator id mismatch and both the execution
	 * as well as the checkpoint have duplicate names.
	 */
	@Test
	public void testRecoveryStateByOperatorNameWhenDuplicateNamesInExecutionAndCheckpoint() throws Exception {
		final int parallelism = 128128;
		Random random = new Random();

		String unDuplicateNameInExecutionAndCheckpoint = StringUtils.getRandomString(random, 5, 500);
		String duplicateNameInExecutionAndCheckpoint = StringUtils.getRandomString(random, 5, 500);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, parallelism, unDuplicateNameInExecutionAndCheckpoint, duplicateNameInExecutionAndCheckpoint, "");
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(unDuplicateNameInExecutionAndCheckpoint, duplicateNameInExecutionAndCheckpoint, "");

		try {
			Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
			fail("Did not throw expected Exception");
		} catch (RestoreCheckpointException expected) {
			assertTrue(findThrowableWithMessage(expected, "allowNonRestoredState").isPresent());
		}
	}

	/**
	 * Tests that savepoint loading does not fails when the operator id mismatch and
	 * only the execution has duplicate names.
	 */
	@Test
	public void testRecoveryStateByOperatorNameWhenDuplicateNamesInExecution() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;
		Random random = new Random();

		String unDuplicateNameInExecution = StringUtils.getRandomString(random, 5, 500);
		String duplicateNameInExecution = StringUtils.getRandomString(random, 5, 500);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, operatorId, parallelism, unDuplicateNameInExecution, "");
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(unDuplicateNameInExecution, duplicateNameInExecution, "");

		Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
	}

	/**
	 * Tests that savepoint loading does not fail when the operator id mismatch and only the checkpoint
	 * has duplicate names.
	 */
	@Test
	public void testRecoveryStateByOperatorNameWhenDuplicateNamesInCheckpoint() throws Exception {
		OperatorID operatorID = new OperatorID();
		final int parallelism = 128128;
		Random random = new Random();

		String unDuplicateNameInCheckpoint = StringUtils.getRandomString(random, 5, 500);
		String duplicateNameInCheckpoint = StringUtils.getRandomString(random, 5, 500);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, parallelism, unDuplicateNameInCheckpoint, duplicateNameInCheckpoint, "");
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(operatorID, parallelism, parallelism + 1, unDuplicateNameInCheckpoint, "");

		final CompletedCheckpoint loaded = Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, true, true, CheckpointProperties.forSavepoint(false));
		assertTrue(loaded.getOperatorStates().containsKey(operatorID));
	}

	/**
	 * Tests that savepoint loading fails when neither operator id nor operator name match.
	 */
	@Test
	public void testRecoveryStateByOperatorNameWhenOperatorIDAndNameMisMatch() throws Exception {
		final int parallelism = 128128;
		Random random = new Random();

		String operatorNameInCheckpoint = StringUtils.getRandomString(random, 5, 500);
		String operatorNameInExecution = StringUtils.getRandomString(random, 5, 500);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, new OperatorID(), parallelism, operatorNameInCheckpoint, "");
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(new OperatorID(), parallelism, parallelism + 1, operatorNameInExecution, "");

		try {
			Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
			fail("Did not throw expected Exception");
		} catch (RestoreCheckpointException expected) {
			assertTrue(findThrowableWithMessage(expected, "allowNonRestoredState").isPresent());
		}
	}

	/**
	 * Tests that savepoint loading does not fail when there is a operator id mismatch but
	 * operator name is match.
	 */
	@Test
	public void testRecoveryStateByOperatorNameWhenOperatorIDMisMatchCheck() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;
		Random random = new Random();

		String operatorName = StringUtils.getRandomString(random, 5, 500);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, operatorId, parallelism, operatorName, "");
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(new OperatorID(), parallelism, parallelism + 1, operatorName, "");

		Checkpoints.loadAndValidateCheckpoint(
			new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
	}

	@Test
	public void testRecoveryStateByOperatorId() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskState(242L, operatorId, parallelism);
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasks(operatorId, parallelism, parallelism + 1);

		Checkpoints.loadAndValidateCheckpoint(
			new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
	}

	@Test
	public void testRecoveryStateByOperatorIdAndType() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;
		Random random = new Random();

		String operatorName = StringUtils.getRandomString(random, 5, 500);
		String operatorType = StringUtils.getRandomString(random, 5, 50);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, operatorId, parallelism, operatorName, operatorType);
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(operatorId, parallelism, parallelism + 1, operatorName, operatorType);

		Checkpoints.loadAndValidateCheckpoint(
			new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
	}

	@Test
	public void testRecoveryStateByOperatorIdWhenTypeIsBlank() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;
		Random random = new Random();

		String operatorName = StringUtils.getRandomString(random, 5, 500);
		String operatorType = StringUtils.getRandomString(random, 5, 50);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, operatorId, parallelism, operatorName, "");
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(operatorId, parallelism, parallelism + 1, operatorName, operatorType);

		Checkpoints.loadAndValidateCheckpoint(
			new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
	}

	@Test
	public void testRecoveryStateByOperatorIdWhenTypeMisMatch() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;
		Random random = new Random();

		String operatorName = StringUtils.getRandomString(random, 5, 500);
		String operatorTypeFromCheckpoint = StringUtils.getRandomString(random, 5, 50);
		String operatorTypeFromExecution = StringUtils.getRandomString(random, 5, 50);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, operatorId, parallelism, operatorName, operatorTypeFromCheckpoint);
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(operatorId, parallelism, parallelism + 1, operatorName, operatorTypeFromExecution);

		Checkpoints.loadAndValidateCheckpoint(
			new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
	}

	@Test
	public void testRecoveryStateByOperatorIdWhenTypeAndNameMisMatch() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;
		Random random = new Random();

		String operatorNameFromCheckpoint = StringUtils.getRandomString(random, 5, 500);
		String operatorNameFromExecution = StringUtils.getRandomString(random, 5, 500);
		String operatorTypeFromCheckpoint = StringUtils.getRandomString(random, 5, 50);
		String operatorTypeFromExecution = StringUtils.getRandomString(random, 5, 50);

		final CompletedCheckpointStorageLocation testSavepoint =
			createSavepointWithOperatorSubtaskStateAndOperationNameAndType(242L, operatorId, parallelism, operatorNameFromCheckpoint, operatorTypeFromCheckpoint);
		final Map<JobVertexID, ExecutionJobVertex> tasks =
			createTasksWithOperatorNameAndType(operatorId, parallelism, parallelism + 1, operatorNameFromExecution, operatorTypeFromExecution);

		try {
			Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false);
			fail("Did not throw expected Exception");
		} catch (RestoreCheckpointException expected) {
			assertTrue(findThrowableWithMessage(expected, "allowNonRestoredState").isPresent());
		}
	}

	/**
	 * Tests that savepoint loading does not fail when there is a max-parallelism mismatch but
	 * skipMaxParallelismCheck is enabled.
	 */
	@Test
	public void testMaxParallelismMismatchWhenSkipMaxParallelismCheck() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;

		final CompletedCheckpointStorageLocation testSavepoint =
				createSavepointWithOperatorSubtaskState(242L, operatorId, parallelism);
		final Map<JobVertexID, ExecutionJobVertex> tasks =
				createTasks(operatorId, parallelism, parallelism + 1);

		Checkpoints.loadAndValidateCheckpoint(
				new JobID(), tasks, testSavepoint, cl, false, true, CheckpointProperties.forSavepoint(false));
	}

	/**
	 * Tests that savepoint with keyed state loading fails when there is a max-parallelism mismatch even though
	 * skipMaxParallelismCheck is enabled.
	 */
	@Test
	public void testMaxParallelismMismatchWhenSkipMaxParallelismCheckButWithKeyedState() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 128128;

		final CompletedCheckpointStorageLocation testSavepoint =
				createSavepointWithOperatorSubtaskStateWithKeyedState(242L, operatorId, parallelism);
		final Map<JobVertexID, ExecutionJobVertex> tasks =
				createTasks(operatorId, parallelism, parallelism + 1);

		try {
			Checkpoints.loadAndValidateCheckpoint(
					new JobID(), tasks, testSavepoint, cl, false);
			fail("Did not throw expected Exception");
		} catch (RestoreCheckpointException expected) {
			assertTrue(findThrowableWithMessage(expected, "Max parallelism mismatch").isPresent());
		}
	}

	/**
	 * Tests that savepoint loading fails when there is non-restored state, but it is not allowed.
	 */
	@Test
	public void testNonRestoredStateWhenDisallowed() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 9;

		final CompletedCheckpointStorageLocation testSavepoint = createSavepointWithOperatorSubtaskState(242L, operatorId, parallelism);
		final Map<JobVertexID, ExecutionJobVertex> tasks = Collections.emptyMap();

		try {
			Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false);
			fail("Did not throw expected Exception");
		} catch (RestoreCheckpointException expected) {
			assertTrue(findThrowableWithMessage(expected, "allowNonRestoredState").isPresent());
		}
	}

	/**
	 * Tests that savepoint loading succeeds when there is non-restored state and it is not allowed.
	 */
	@Test
	public void testNonRestoredStateWhenAllowed() throws Exception {
		final OperatorID operatorId = new OperatorID();
		final int parallelism = 9;

		final CompletedCheckpointStorageLocation testSavepoint = createSavepointWithOperatorSubtaskState(242L, operatorId, parallelism);
		final Map<JobVertexID, ExecutionJobVertex> tasks = Collections.emptyMap();

		final CompletedCheckpoint loaded = Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, true);

		assertTrue(loaded.getOperatorStates().isEmpty());
	}

	/**
	 * Tests that savepoint loading fails when there is non-restored coordinator state only,
	 * and non-restored state is not allowed.
	 */
	@Test
	public void testUnmatchedCoordinatorOnlyStateFails() throws Exception {
		final OperatorID operatorID = new OperatorID();
		final int maxParallelism = 1234;

		final OperatorState state = new OperatorState(operatorID, maxParallelism / 2, maxParallelism);
		state.setCoordinatorState(new ByteStreamStateHandle("coordinatorState", new byte[0]));

		final CompletedCheckpointStorageLocation testSavepoint = createSavepointWithOperatorState(42L, state, Collections.emptyMap());
		final Map<JobVertexID, ExecutionJobVertex> tasks = Collections.emptyMap();

		try {
			Checkpoints.loadAndValidateCheckpoint(new JobID(), tasks, testSavepoint, cl, false);
			fail("Did not throw expected Exception");
		} catch (RestoreCheckpointException expected) {
			assertTrue(findThrowableWithMessage(expected, "allowNonRestoredState").isPresent());
		}
	}

	// ------------------------------------------------------------------------
	//  setup utils
	// ------------------------------------------------------------------------

	private static CompletedCheckpointStorageLocation createSavepointWithOperatorState(
			final long checkpointId,
			final OperatorState state,
			Map<OperatorID, Tuple2<String, String>> operatorNameAndTypes) throws IOException {

		final CheckpointMetadata savepoint = new CheckpointMetadata(checkpointId, Collections.singletonList(state), Collections.emptyList(), operatorNameAndTypes);
		final StreamStateHandle serializedMetadata;

		try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
			Checkpoints.storeCheckpointMetadata(savepoint, os);
			serializedMetadata = new ByteStreamStateHandle("checkpoint", os.toByteArray());
		}

		return new TestCompletedCheckpointStorageLocation(serializedMetadata, "dummy/pointer");
	}

	private static CompletedCheckpointStorageLocation createSavepointWithOperatorSubtaskStateWithKeyedState(
			final long checkpointId,
			final OperatorID operatorId,
			final int parallelism) throws IOException {

		final Random rnd = new Random();

		final OperatorSubtaskState subtaskState = OperatorSubtaskState.builder()
			.setManagedOperatorState(new OperatorStreamStateHandle(
				Collections.emptyMap(),
				new ByteStreamStateHandle("testHandler", new byte[0])))
			.setManagedKeyedState(new KeyGroupsStateHandle(
				new KeyGroupRangeOffsets(0, 1),
				new ByteStreamStateHandle("testHandle", new byte[0])))
			.setInputChannelState(singleton(createNewInputChannelStateHandle(10, rnd)))
			.setResultSubpartitionState(singleton(createNewResultSubpartitionStateHandle(10, rnd)))
			.build();

		final OperatorState state = new OperatorState(operatorId, parallelism, parallelism);
		state.putState(0, subtaskState);

		return createSavepointWithOperatorState(checkpointId, state, Collections.emptyMap());
	}

	private CompletedCheckpointStorageLocation createSavepointWithOperatorSubtaskStateAndOperationNameAndType(
		long checkpointId,
		int parallelism,
		String unDuplicatedOperatorName,
		String duplicatedOperatorName,
		String operatorType) throws IOException {
		final Random rnd = new Random();
		final OperatorSubtaskState subtaskState = OperatorSubtaskState.builder()
			.setManagedOperatorState(new OperatorStreamStateHandle(
				Collections.emptyMap(),
				new ByteStreamStateHandle("testHandler", new byte[0])))
			.setInputChannelState(singleton(createNewInputChannelStateHandle(10, rnd)))
			.setResultSubpartitionState(singleton(createNewResultSubpartitionStateHandle(10, rnd)))
			.build();

		Collection<OperatorState> operatorStates = new ArrayList<>();
		OperatorID firstOperatorId = new OperatorID();
		final OperatorState firstState = new OperatorState(firstOperatorId, parallelism, parallelism);
		firstState.putState(0, subtaskState);

		OperatorID secondOperatorId = new OperatorID();
		final OperatorState secondState = new OperatorState(secondOperatorId, parallelism, parallelism);
		secondState.putState(0, subtaskState);

		OperatorID thirdOperatorId = new OperatorID();
		final OperatorState thirdState = new OperatorState(thirdOperatorId, parallelism, parallelism);
		thirdState.putState(0, subtaskState);

		operatorStates.add(firstState);
		operatorStates.add(secondState);
		operatorStates.add(thirdState);

		Map<OperatorID, Tuple2<String, String>> operatorNameAndTypes = new HashMap<>();
		operatorNameAndTypes.put(firstOperatorId, new Tuple2<>(unDuplicatedOperatorName, operatorType));
		operatorNameAndTypes.put(secondOperatorId, new Tuple2<>(duplicatedOperatorName, operatorType));
		operatorNameAndTypes.put(thirdOperatorId, new Tuple2<>(duplicatedOperatorName, operatorType));

		return createSavepointWithOperatorState(checkpointId, operatorStates, operatorNameAndTypes);
	}

	private static CompletedCheckpointStorageLocation createSavepointWithOperatorState(
		final long checkpointId,
		final Collection<OperatorState> operatorStates,
		Map<OperatorID, Tuple2<String, String>> operatorNameAndTypes) throws IOException {
		final CheckpointMetadata savepoint = new CheckpointMetadata(checkpointId, operatorStates, Collections.emptyList(), operatorNameAndTypes);
		final StreamStateHandle serializedMetadata;

		try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
			Checkpoints.storeCheckpointMetadata(savepoint, os);
			serializedMetadata = new ByteStreamStateHandle("checkpoint", os.toByteArray());
		}

		return new TestCompletedCheckpointStorageLocation(serializedMetadata, "dummy/pointer");
	}

	private CompletedCheckpointStorageLocation createSavepointWithOperatorSubtaskStateAndOperationNameAndType(
			long checkpointId,
			OperatorID operatorId,
			int parallelism,
			String operatorName,
			String operatorType) throws IOException {
		final Random rnd = new Random();

		final OperatorSubtaskState subtaskState = OperatorSubtaskState.builder()
			.setManagedOperatorState(new OperatorStreamStateHandle(
				Collections.emptyMap(),
				new ByteStreamStateHandle("testHandler", new byte[0])))
			.setInputChannelState(singleton(createNewInputChannelStateHandle(10, rnd)))
			.setResultSubpartitionState(singleton(createNewResultSubpartitionStateHandle(10, rnd)))
			.build();

		final OperatorState state = new OperatorState(operatorId, parallelism, parallelism);
		state.putState(0, subtaskState);

		Map<OperatorID, Tuple2<String, String>> operatorNameAndTypes = Collections.singletonMap(operatorId, new Tuple2<>(operatorName, operatorType));

		return createSavepointWithOperatorState(checkpointId, state, operatorNameAndTypes);
	}

	private static CompletedCheckpointStorageLocation createSavepointWithOperatorSubtaskState(
			final long checkpointId,
			final OperatorID operatorId,
			final int parallelism) throws IOException {

		final Random rnd = new Random();

		final OperatorSubtaskState subtaskState = OperatorSubtaskState.builder()
			.setManagedOperatorState(new OperatorStreamStateHandle(
				Collections.emptyMap(),
				new ByteStreamStateHandle("testHandler", new byte[0])))
			.setInputChannelState(singleton(createNewInputChannelStateHandle(10, rnd)))
			.setResultSubpartitionState(singleton(createNewResultSubpartitionStateHandle(10, rnd)))
			.build();

		final OperatorState state = new OperatorState(operatorId, parallelism, parallelism);
		state.putState(0, subtaskState);

		return createSavepointWithOperatorState(checkpointId, state, Collections.emptyMap());
	}

	private static Map<JobVertexID, ExecutionJobVertex> createTasks(OperatorID operatorId, int parallelism, int maxParallelism) {
		final JobVertexID vertexId = new JobVertexID(operatorId.getLowerPart(), operatorId.getUpperPart());

		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(parallelism);
		when(vertex.getMaxParallelism()).thenReturn(maxParallelism);
		when(vertex.getOperatorIDs()).thenReturn(Collections.singletonList(OperatorIDPair.generatedIDOnly(operatorId)));

		if (parallelism != maxParallelism) {
			when(vertex.isMaxParallelismConfigured()).thenReturn(true);
		}

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(vertexId, vertex);

		return tasks;
	}

	private static Map<JobVertexID, ExecutionJobVertex> createTasksWithOperatorNameAndType(
			OperatorID operatorId,
			int parallelism,
			int maxParallelism,
			String operatorName,
			String operatorType) throws JobException, JobExecutionException {
		ExecutionGraph graph = TestingExecutionGraphBuilder.newBuilder().build();
		graph.setOperatorNameAndTypes(Collections.singletonMap(operatorId, new Tuple2<>(operatorName, operatorType)));

		JobVertex jobVertex = new JobVertex(
			operatorId.toHexString(),
			new JobVertexID(),
			singletonList(OperatorIDPair.of(operatorId, new OperatorID())));

		ExecutionJobVertex executionJobVertex = new ExecutionJobVertex(graph, jobVertex, new Configuration(), 1, 1, Time.seconds(1), 1L, 1L);

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertex.getID(), executionJobVertex);

		return tasks;
	}

	private static Map<JobVertexID, ExecutionJobVertex> createTasksWithOperatorNameAndType(
		String unDuplicatedOperatorName,
		String duplicatedOperatorName,
		String operatorType) throws JobException, JobExecutionException {
		OperatorID firstOperatorId = new OperatorID();
		OperatorID secondOperatorId = new OperatorID();
		OperatorID thirdOperatorId = new OperatorID();

		ExecutionGraph graph = TestingExecutionGraphBuilder.newBuilder().build();

		Map<OperatorID, Tuple2<String, String>> operatorNameAndTypes = new HashMap<>();
		operatorNameAndTypes.put(firstOperatorId, new Tuple2<>(unDuplicatedOperatorName, operatorType));
		operatorNameAndTypes.put(secondOperatorId, new Tuple2<>(duplicatedOperatorName, operatorType));
		operatorNameAndTypes.put(thirdOperatorId, new Tuple2<>(duplicatedOperatorName, operatorType));

		graph.setOperatorNameAndTypes(operatorNameAndTypes);

		List<OperatorIDPair> operatorIDs = new ArrayList<>();
		operatorIDs.add(OperatorIDPair.of(firstOperatorId, new OperatorID()));
		operatorIDs.add(OperatorIDPair.of(secondOperatorId, new OperatorID()));
		operatorIDs.add(OperatorIDPair.of(thirdOperatorId, new OperatorID()));

		JobVertex jobVertex = new JobVertex(firstOperatorId.toHexString(), new JobVertexID(), operatorIDs);

		ExecutionJobVertex executionJobVertex = new ExecutionJobVertex(graph, jobVertex, new Configuration(), 1, 1, Time.seconds(1), 1L, 1L);

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(jobVertex.getID(), executionJobVertex);

		return tasks;
	}
}
