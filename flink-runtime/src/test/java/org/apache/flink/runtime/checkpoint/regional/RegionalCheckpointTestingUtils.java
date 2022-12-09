package org.apache.flink.runtime.checkpoint.regional;

import com.sun.istack.Nullable;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTrackerTest;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;

public class RegionalCheckpointTestingUtils {

    public static ExecutionGraph createExecutionGraphWithSingleVertexAndEnableCheckpointing(
            int parallelism,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store) throws Exception {
        return createExecutionGraphWithSingleVertexAndEnableCheckpointing(parallelism, counter, store, null);
    }

    public static ExecutionGraph createExecutionGraphWithSingleVertexAndEnableCheckpointing(
            int parallelism,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store,
            @Nullable ScheduledExecutor timer) throws Exception {
        final Time timeout = Time.days(1L);

        JobVertex jobVertex = new JobVertex("MockVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);
        jobVertex.setParallelism(parallelism);

        final ExecutionGraph executionGraph = TestingExecutionGraphBuilder
                .newBuilder()
                .setJobGraph(new JobGraph(jobVertex))
                .setRpcTimeout(timeout)
                .setAllocationTimeout(timeout)
                .build();

        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        enableCheckpointing(executionGraph, counter, store, timer);

        CheckpointCoordinator coordinator = executionGraph.getCheckpointCoordinator();
        RegionalCheckpointHandler handler = new RegionalCheckpointHandler(coordinator, executionGraph.getJobID(),
                new DefaultCheckpointTopology(executionGraph, false), 2, 0.5d);
        coordinator.setCheckpointHandler(handler);
        return executionGraph;
    }

    public static ExecutionGraph createExecutionGraphWithTwoAllToAllConnectedVerticesAndEnableCheckpointing(
            int parallelism,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store) throws Exception {
        return createExecutionGraphWithTwoAllToAllConnectedVerticesAndEnableCheckpointing(parallelism, counter, store, null);
    }

    public static ExecutionGraph createExecutionGraphWithTwoAllToAllConnectedVerticesAndEnableCheckpointing(
            int parallelism,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store,
            @Nullable ScheduledExecutor timer) throws Exception {
        final Time timeout = Time.days(1L);

        JobVertex[] jobVertices = new JobVertex[2];
        jobVertices[0] = createNoOpVertex("v1", parallelism);
        jobVertices[1] = createNoOpVertex("v2", parallelism);
        jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);

        final ExecutionGraph executionGraph = TestingExecutionGraphBuilder
                .newBuilder()
                .setJobGraph(new JobGraph(jobVertices))
                .setRpcTimeout(timeout)
                .setAllocationTimeout(timeout)
                .build();

        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        enableCheckpointing(executionGraph, counter, store, timer);

        return executionGraph;
    }

    public static ExecutionGraph createExecutionGraphWithForwardConnectedVerticesAndEnableCheckpointing(
            int parallelism,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store) throws Exception {
        return createExecutionGraphWithPointWiseConnectedVerticesAndEnableCheckpointing(
                parallelism,
                parallelism,
                counter,
                store,
                null);
    }

    public static ExecutionGraph createExecutionGraphWithRescaleConnectedVerticesAndEnableCheckpointing(
            int upStreamParallelism,
            int downStreamParallelism,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store) throws Exception {
        return createExecutionGraphWithPointWiseConnectedVerticesAndEnableCheckpointing(
                upStreamParallelism,
                downStreamParallelism,
                counter,
                store,
                null);
    }

    public static ExecutionGraph createExecutionGraphWithPointWiseConnectedVerticesAndEnableCheckpointing(
            int upStreamParallelism,
            int dowStreamParallelism,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store,
            @Nullable ScheduledExecutor timer) throws Exception {
        final Time timeout = Time.days(1L);

        JobVertex[] jobVertices = new JobVertex[2];
        jobVertices[0] = createNoOpVertex("v1", upStreamParallelism);
        jobVertices[1] = createNoOpVertex("v2", dowStreamParallelism);
        jobVertices[1].connectNewDataSetAsInput(jobVertices[0], POINTWISE, PIPELINED);

        final ExecutionGraph executionGraph = TestingExecutionGraphBuilder
                .newBuilder()
                .setJobGraph(new JobGraph(jobVertices))
                .setRpcTimeout(timeout)
                .setAllocationTimeout(timeout)
                .build();

        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        enableCheckpointing(executionGraph, counter, store, timer);

        return executionGraph;
    }

    public static ExecutionGraph createExecutionGraphWithBroadcastConnectedVerticesAndEnableCheckpointing(
            int upStreamParallelism,
            int dowStreamParallelism,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store,
            @Nullable ScheduledExecutor timer) throws Exception {
        final Time timeout = Time.days(1L);

        JobVertex[] jobVertices = new JobVertex[2];
        jobVertices[0] = createNoOpVertex("v1", upStreamParallelism);
        jobVertices[1] = createNoOpVertex("v2", dowStreamParallelism);
        jobVertices[1].addBroadcastedInput(jobVertices[0]);
        jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);

        final ExecutionGraph executionGraph = TestingExecutionGraphBuilder
                .newBuilder()
                .setJobGraph(new JobGraph(jobVertices))
                .setRpcTimeout(timeout)
                .setAllocationTimeout(timeout)
                .build();

        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        enableCheckpointing(executionGraph, counter, store, timer);

        return executionGraph;
    }

    private static void enableCheckpointing(
            ExecutionGraph graph,
            CheckpointIDCounter counter,
            CompletedCheckpointStore store,
            @Nullable ScheduledExecutor timer) {

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(100)
                        .setCheckpointTimeout(100)
                        .setMinPauseBetweenCheckpoints(100)
                        .setMaxConcurrentCheckpoints(1)
                        .setCheckpointRetentionPolicy(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION)
                        .setExactlyOnce(true)
                        .setUnalignedCheckpointsEnabled(false)
                        .setPreferCheckpointForRecovery(false)
                        .setTolerableCheckpointFailureNumber(0)
                        .setRegionalCheckpointEnabled(true)
                        .setMaxTolerableConsecutiveFailuresOrExpirations(2)
                        .setMaxTolerableFailureOrExpirationRatio(0.5)
                        .build();


        // collect the vertices that receive "trigger checkpoint" messages.
        // currently, these are all the sources
        List<ExecutionJobVertex> triggerVertices = new ArrayList<>();

        // collect the vertices that need to acknowledge the checkpoint
        // currently, these are all vertices
        List<ExecutionJobVertex> ackVertices = new ArrayList<>();

        // collect the vertices that receive "commit checkpoint" messages
        // currently, these are all vertices
        List<ExecutionJobVertex> commitVertices = new ArrayList<>();

        graph.getAllExecutionVertices().forEach(ev -> {
            if (ev.getJobVertex().getJobVertex().isInputVertex()) {
                triggerVertices.add(ev.getJobVertex());
            }
            ackVertices.add(ev.getJobVertex());
            commitVertices.add(ev.getJobVertex());
        });

        graph.enableCheckpointing(
                chkConfig,
                triggerVertices,
                ackVertices,
                commitVertices,
                Collections.emptyList(),
                Collections.emptyMap(),
                counter,
                store,
                new MemoryStateBackend(),
                CheckpointStatsTrackerTest.createTestTracker());

        if (timer != null) {
            Preconditions.checkNotNull(graph.getCheckpointCoordinator());
            graph.getCheckpointCoordinator().setTimer(timer);
        }
    }

    public static final class TestingCheckpointIDCounter implements CheckpointIDCounter {

        private final CompletableFuture<JobStatus> shutdownStatus;

        public TestingCheckpointIDCounter(CompletableFuture<JobStatus> shutdownStatus) {
            this.shutdownStatus = shutdownStatus;
        }

        @Override
        public void start() {}

        @Override
        public void shutdown(JobStatus jobStatus) {
            shutdownStatus.complete(jobStatus);
        }

        @Override
        public long getAndIncrement() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public long get() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void setCount(long newId) {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    public static final class TestingCompletedCheckpointStore implements CompletedCheckpointStore {

        private final CompletableFuture<JobStatus> shutdownStatus;

        public TestingCompletedCheckpointStore(CompletableFuture<JobStatus> shutdownStatus) {
            this.shutdownStatus = shutdownStatus;
        }

        @Override
        public void recover() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void addCheckpoint(CompletedCheckpoint checkpoint) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public CompletedCheckpoint getLatestCheckpoint(boolean isPreferCheckpointForRecovery) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void shutdown(JobStatus jobStatus) {
            shutdownStatus.complete(jobStatus);
        }

        @Override
        public List<CompletedCheckpoint> getAllCheckpoints() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public int getNumberOfRetainedCheckpoints() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public int getMaxNumberOfRetainedCheckpoints() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public boolean requiresExternalizedCheckpoints() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }
}
