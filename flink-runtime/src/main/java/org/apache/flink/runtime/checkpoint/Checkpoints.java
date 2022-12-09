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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializer;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializers;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinator.SKIP_MAX_PARALLELISM_CHECK_DEFAULT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class with the methods to write/load/dispose the checkpoint and savepoint metadata.
 *
 * <p>Stored checkpoint metadata files have the following format:
 * <pre>[MagicNumber (int) | Format Version (int) | Checkpoint Metadata (variable)]</pre>
 *
 * <p>The actual savepoint serialization is version-specific via the {@link MetadataSerializer}.
 */
public class Checkpoints {

	private static final Logger LOG = LoggerFactory.getLogger(Checkpoints.class);

	/** Magic number at the beginning of every checkpoint metadata file, for sanity checks. */
	public static final int HEADER_MAGIC_NUMBER = 0x4960672d;

	// ------------------------------------------------------------------------
	//  Writing out checkpoint metadata
	// ------------------------------------------------------------------------

	public static void storeCheckpointMetadata(
			CheckpointMetadata checkpointMetadata,
			OutputStream out) throws IOException {

		DataOutputStream dos = new DataOutputStream(out);
		storeCheckpointMetadata(checkpointMetadata, dos);
	}

	public static void storeCheckpointMetadata(
			CheckpointMetadata checkpointMetadata,
			DataOutputStream out) throws IOException {

		// write generic header
		out.writeInt(HEADER_MAGIC_NUMBER);

		out.writeInt(MetadataV3Serializer.VERSION);
		MetadataV3Serializer.serialize(checkpointMetadata, out);
	}

	// ------------------------------------------------------------------------
	//  Reading and validating checkpoint metadata
	// ------------------------------------------------------------------------

	public static CheckpointMetadata loadCheckpointMetadata(DataInputStream in, ClassLoader classLoader, String externalPointer) throws IOException {
		checkNotNull(in, "input stream");
		checkNotNull(classLoader, "classLoader");

		final int magicNumber = in.readInt();

		if (magicNumber == HEADER_MAGIC_NUMBER) {
			final int version = in.readInt();
			final MetadataSerializer serializer = MetadataSerializers.getSerializer(version);
			return serializer.deserialize(in, classLoader, externalPointer);
		}
		else {
			throw new IOException("Unexpected magic number. This can have multiple reasons: " +
					"(1) You are trying to load a Flink 1.0 savepoint, which is not supported by this " +
					"version of Flink. (2) The file you were pointing to is not a savepoint at all. " +
					"(3) The savepoint file has been corrupted.");
		}
	}

	public static CompletedCheckpoint loadAndValidateCheckpoint(
			JobID jobId,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			CompletedCheckpointStorageLocation location,
			ClassLoader classLoader,
			boolean allowNonRestoredState)
			throws IOException {
		return loadAndValidateCheckpoint(
				jobId,
				tasks,
				location,
				classLoader,
				allowNonRestoredState,
				SKIP_MAX_PARALLELISM_CHECK_DEFAULT,
				CheckpointProperties.forSavepoint(false));
	}

	public static CompletedCheckpoint loadAndValidateCheckpoint(
			JobID jobId,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			CompletedCheckpointStorageLocation location,
			ClassLoader classLoader,
			boolean allowNonRestoredState,
			boolean skipMaxParallelismCheck,
			CheckpointProperties checkpointProperties) throws IOException {

		checkNotNull(jobId, "jobId");
		checkNotNull(tasks, "tasks");
		checkNotNull(location, "location");
		checkNotNull(classLoader, "classLoader");

		final StreamStateHandle metadataHandle = location.getMetadataHandle();
		final String checkpointPointer = location.getExternalPointer();

		// (1) load the savepoint
		final CheckpointMetadata checkpointMetadata;
		try (InputStream in = metadataHandle.openInputStream()) {
			DataInputStream dis = new DataInputStream(in);
			checkpointMetadata = loadCheckpointMetadata(dis, classLoader, checkpointPointer);
		} catch (Exception e) {
			throw new RestoreCheckpointException(RestoreFailureReason.LOAD_METADATA_EXCEPTION, e);
		}

		Map<OperatorID, Tuple2<String, String>> operatorNameAndTypesFromCheckpoint =
			checkpointMetadata.getOperatorNameAndTypes();

		Map<OperatorID, Tuple2<String, String>> operatorNameAndTypesFromExecution = new HashMap<>();
		tasks.values().stream().findFirst().ifPresent(executionJobVertex -> {
			ExecutionGraph graph = executionJobVertex.getGraph();
			if (graph != null) {
				operatorNameAndTypesFromExecution.putAll(graph.getOperatorNameAndTypes());
			}
		});

		// generate mapping from operator to task
		Map<OperatorID, Tuple2<String, ExecutionJobVertex>> operatorToJobVertexMapping = new HashMap<>();
		for (ExecutionJobVertex task : tasks.values()) {
			for (OperatorIDPair operatorIDPair : task.getOperatorIDs()) {
				OperatorID operatorID = operatorIDPair.getGeneratedOperatorID();
				operatorToJobVertexMapping.put(operatorID,
					generatedWithType(operatorID, operatorNameAndTypesFromExecution, task));
				operatorIDPair.getUserDefinedOperatorID()
					.ifPresent(id -> operatorToJobVertexMapping.put(id,
						generatedWithType(id, operatorNameAndTypesFromExecution, task)));
			}
		}

		Map<String, OperatorID> name2OperatorIDs = new HashMap<>();

		if (MapUtils.isNotEmpty(operatorNameAndTypesFromExecution)) {
			operatorNameAndTypesFromExecution.forEach(((operatorID, names) ->
				name2OperatorIDs.put(names.f0, operatorID)));
		}

		// (2) validate it (parallelism, etc)
		// Operator maps state to new task according to the following rules:
		// 1. map by operator id when type is blank
		// 2. map by operator id and type when type is not blank
		// 3. map by operator name when state was not matched in the previous two steps
		// if all the above do not match, it is decided by 'allowNonRestoredState'
		Set<String> duplicatedOperatorNamesInCheckpoint= getDuplicatedOperatorNames(operatorNameAndTypesFromCheckpoint);
		Set<String> duplicatedOperatorNamesInExecution= getDuplicatedOperatorNames(operatorNameAndTypesFromExecution);

		HashMap<OperatorID, OperatorState> operatorStates = new HashMap<>(checkpointMetadata.getOperatorStates().size());
		for (OperatorState operatorState : checkpointMetadata.getOperatorStates()) {
			OperatorID operatorIDFromCheckpoint = operatorState.getOperatorID();
			OperatorID restoreStateOperatorID = operatorIDFromCheckpoint;
			// operator name and type from checkpoint
			Tuple2<String, String> operatorNameAndType =
				operatorNameAndTypesFromCheckpoint.getOrDefault(operatorIDFromCheckpoint, Tuple2.of("", ""));
			// operator type and JobVertex from Execution
			Tuple2<String, ExecutionJobVertex> typeAndExecutionJobVertex = operatorToJobVertexMapping.get(operatorIDFromCheckpoint);
			ExecutionJobVertex executionJobVertex = null;
			if (needRestoreByOperatorId(operatorNameAndType, typeAndExecutionJobVertex)) {
				// 1. map by operator id when type is blank
				LOG.info("Operator ({}) restore by operator id {} without operator type check.", operatorNameAndType.f0, operatorIDFromCheckpoint);
				executionJobVertex = typeAndExecutionJobVertex.f1;
			} else if (needRestoreByOperatorIdAndType(operatorNameAndType, typeAndExecutionJobVertex)) {
				// 2. map by operator id and type when type is not blank
				LOG.info("Operator ({}) restore by operator id {} and type {}.", operatorNameAndType.f0,
					operatorIDFromCheckpoint, operatorNameAndType.f1);
				executionJobVertex = typeAndExecutionJobVertex.f1;
			} else if (needRestoreByOperatorName(name2OperatorIDs, duplicatedOperatorNamesInCheckpoint,
				duplicatedOperatorNamesInExecution, operatorNameAndType)) {
				// 3. maps by operator name
				String operatorNameFromCheckpoint = operatorNameAndType.f0;
				OperatorID operatorIDByName = name2OperatorIDs.get(operatorNameFromCheckpoint);
				if (operatorIDByName != null) {
					executionJobVertex = operatorToJobVertexMapping.get(operatorIDByName).f1;
					restoreStateOperatorID = operatorIDByName;
					LOG.info("Operator ({}) restore by operator name, " +
							"checkpointOperatorID is: {}, executionOperatorID is: {}",
						operatorNameFromCheckpoint, operatorIDFromCheckpoint, operatorIDByName);
				} else {
					LOG.warn("Operator ({}) can not restore by operator name, " +
						"because the operator name does not exist in the execution", operatorNameFromCheckpoint);
				}
			}

			if (executionJobVertex != null) {
				if (executionJobVertex.getMaxParallelism() == operatorState.getMaxParallelism()
						|| !executionJobVertex.isMaxParallelismConfigured()) {
					operatorStates.put(restoreStateOperatorID, operatorState);
				} else {
					boolean hasKeyedState = StateUtil.hasKeyedState(operatorState);
					if (hasKeyedState || !skipMaxParallelismCheck) {
						String msg =
								String.format(
										"Failed to rollback to checkpoint/savepoint %s. "
												+ "Max parallelism mismatch between checkpoint/savepoint state and "
												+ "new program. Cannot map operator %s (%s) with max parallelism %d to new "
												+ "program with max parallelism %d. This indicates that the program "
												+ "has been changed in a non-compatible way after the "
												+ "checkpoint/savepoint.",
										checkpointMetadata,
										operatorNameAndType.f0,
										operatorIDFromCheckpoint,
										operatorState.getMaxParallelism(),
										executionJobVertex.getMaxParallelism());

						throw new RestoreCheckpointException(
							RestoreFailureReason.MAXIMUM_PARALLELISM_MISMATCH,
							new IllegalStateException(msg));
					} else {
						LOG.info("Skip max parallelism check.");
					}
				}
			} else if (allowNonRestoredState) {
				LOG.info("Skipping savepoint state for operator {}.", operatorIDFromCheckpoint);
			} else {
				if (operatorState.getCoordinatorState() != null) {
					throwNonRestoredStateException(checkpointPointer, operatorIDFromCheckpoint);
				}

				for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
					if (operatorSubtaskState.hasState()) {
						throwNonRestoredStateException(checkpointPointer, operatorIDFromCheckpoint);
					}
				}

				LOG.info("Skipping empty savepoint state for operator {}.", operatorIDFromCheckpoint);
			}
		}

		return new CompletedCheckpoint(
				jobId,
				checkpointMetadata.getCheckpointId(),
				0L,
				0L,
				operatorStates,
				checkpointMetadata.getMasterStates(),
				checkpointProperties,
				location);
	}

	private static boolean needRestoreByOperatorId(
		Tuple2<String, String> operatorNameAndType,
		Tuple2<String, ExecutionJobVertex> typeAndExecutionJobVertex) {
		return typeAndExecutionJobVertex != null
			&& (StringUtils.isEmpty(operatorNameAndType.f1) || StringUtils.isEmpty(typeAndExecutionJobVertex.f0));
	}

	private static boolean needRestoreByOperatorIdAndType(
			Tuple2<String, String> operatorNameAndType,
			Tuple2<String, ExecutionJobVertex> typeAndExecutionJobVertex) {
		return typeAndExecutionJobVertex != null
			&& StringUtils.isNotEmpty(operatorNameAndType.f1)
			&& Objects.equals(operatorNameAndType.f1, typeAndExecutionJobVertex.f0);
	}

	private static boolean needRestoreByOperatorName(
		Map<String, OperatorID> name2OperatorIDs,
		Set<String> duplicatedOperatorNamesInCheckpoint,
		Set<String> duplicatedOperatorNamesInExecution,
		Tuple2<String, String> operatorNameAndType) {
		return MapUtils.isNotEmpty(name2OperatorIDs)
			&& StringUtils.isNotEmpty(operatorNameAndType.f0)
			&& !duplicatedOperatorNamesInCheckpoint.contains(operatorNameAndType.f0)
			&& !duplicatedOperatorNamesInExecution.contains(operatorNameAndType.f0);
	}

	private static Tuple2<String, ExecutionJobVertex> generatedWithType(
			OperatorID operatorID,
			Map<OperatorID, Tuple2<String, String>> operatorNameAndTypes,
			ExecutionJobVertex task) {
		Tuple2<String, String> operatorNameAndType = operatorNameAndTypes.get(operatorID);
		String operatorType = operatorNameAndType == null ? "" : operatorNameAndType.f1;
		return Tuple2.of(operatorType, task);
	}

	private static Set<String> getDuplicatedOperatorNames(
		Map<OperatorID, Tuple2<String, String>> operatorNameAndTypes) {
		return operatorNameAndTypes.values()
			.stream()
			.map(names -> names.f0)
			.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
			.entrySet()
			.stream()
			.filter(x -> x.getValue() > 1)
			.map(Map.Entry::getKey)
			.collect(Collectors.toSet());
	}

	private static void throwNonRestoredStateException(String checkpointPointer, OperatorID operatorId) {
		String msg = String.format("Failed to rollback to checkpoint/savepoint %s. " +
						"Cannot map checkpoint/savepoint state for operator %s to the new program, " +
						"because the operator is not available in the new program. If " +
						"you want to allow to skip this, you can set the --allowNonRestoredState " +
						"option on the CLI.",
				checkpointPointer, operatorId);

		throw new RestoreCheckpointException(
			RestoreFailureReason.OPERATOR_STATE_MISMATCH,
			new IllegalStateException(msg));
	}

	// ------------------------------------------------------------------------
	//  Savepoint Disposal Hooks
	// ------------------------------------------------------------------------

	public static void disposeSavepoint(
			String pointer,
			StateBackend stateBackend,
			ClassLoader classLoader) throws IOException, FlinkException {

		checkNotNull(pointer, "location");
		checkNotNull(stateBackend, "stateBackend");
		checkNotNull(classLoader, "classLoader");

		final CompletedCheckpointStorageLocation checkpointLocation = stateBackend.resolveCheckpoint(pointer);

		final StreamStateHandle metadataHandle = checkpointLocation.getMetadataHandle();

		// load the savepoint object (the metadata) to have all the state handles that we need
		// to dispose of all state
		final CheckpointMetadata metadata;
		try (InputStream in = metadataHandle.openInputStream();
			DataInputStream dis = new DataInputStream(in)) {

			metadata = loadCheckpointMetadata(dis, classLoader, pointer);
		}

		Exception exception = null;

		// first dispose the savepoint metadata, so that the savepoint is not
		// addressable any more even if the following disposal fails
		try {
			metadataHandle.discardState();
		}
		catch (Exception e) {
			exception = e;
		}

		// now dispose the savepoint data
		try {
			metadata.dispose();
		}
		catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		// now dispose the location (directory, table, whatever)
		try {
			checkpointLocation.disposeStorageLocation();
		}
		catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		// forward exceptions caught in the process
		if (exception != null) {
			ExceptionUtils.rethrowIOException(exception);
		}
	}

	public static void disposeSavepoint(
			String pointer,
			Configuration configuration,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IOException, FlinkException {

		checkNotNull(pointer, "location");
		checkNotNull(configuration, "configuration");
		checkNotNull(classLoader, "classLoader");

		StateBackend backend = loadStateBackend(configuration, classLoader, logger);

		disposeSavepoint(pointer, backend, classLoader);
	}

	@Nonnull
	public static StateBackend loadStateBackend(Configuration configuration, ClassLoader classLoader, @Nullable Logger logger) {
		if (logger != null) {
			logger.info("Attempting to load configured state backend for savepoint disposal");
		}

		StateBackend backend = null;
		try {
			backend = StateBackendLoader.loadStateBackendFromConfig(configuration, classLoader, null);

			if (backend == null && logger != null) {
				logger.info("No state backend configured, attempting to dispose savepoint " +
						"with default backend (file system based)");
			}
		}
		catch (Throwable t) {
			// catches exceptions and errors (like linking errors)
			if (logger != null) {
				logger.info("Could not load configured state backend.");
				logger.debug("Detailed exception:", t);
			}
		}

		if (backend == null) {
			// We use the memory state backend by default. The MemoryStateBackend is actually
			// FileSystem-based for metadata
			backend = new MemoryStateBackend();
		}
		return backend;
	}

	// ------------------------------------------------------------------------

	/** This class contains only static utility methods and is not meant to be instantiated. */
	private Checkpoints() {}
}
