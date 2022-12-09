package org.apache.flink.runtime.checkpoint.regional;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.regional.RegionalCheckpointTestingUtils.TestingCompletedCheckpointStore;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.checkpoint.regional.RegionalCheckpointTestingUtils.createExecutionGraphWithSingleVertexAndEnableCheckpointing;
import static org.apache.flink.runtime.checkpoint.regional.RegionalCheckpointTestingUtils.createExecutionGraphWithTwoAllToAllConnectedVerticesAndEnableCheckpointing;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class DefaultCheckpointTopologyTest {

    private static final boolean DUMMY_EXCLUDE_BROADCASTED_VERTEX = false;

    @Test
    public void testSingleVertexWithParallelismOne() throws Exception {

        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        ExecutionGraph graph = createExecutionGraphWithSingleVertexAndEnableCheckpointing(1, counter, store);

        DefaultCheckpointTopology checkpointTopology =
                new DefaultCheckpointTopology(graph, DUMMY_EXCLUDE_BROADCASTED_VERTEX);

        assertNotNull(checkpointTopology.getCheckpointRegions());
        assertThat(checkpointTopology.getCheckpointRegions().size(), equalTo(1));
    }

    @Test
    public void testSingleVertexWithParallelismTwo() throws Exception {

        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        ExecutionGraph graph = createExecutionGraphWithSingleVertexAndEnableCheckpointing(2, counter, store);

        DefaultCheckpointTopology checkpointTopology =
                new DefaultCheckpointTopology(graph, DUMMY_EXCLUDE_BROADCASTED_VERTEX);

        assertNotNull(checkpointTopology.getCheckpointRegions());
        assertThat(checkpointTopology.getCheckpointRegions().size(), equalTo(2));
    }

    @Test
    public void testTwoAllToAllConnectedVerticesWithParallelismOne() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        ExecutionGraph graph = createExecutionGraphWithTwoAllToAllConnectedVerticesAndEnableCheckpointing(1, counter, store);

        DefaultCheckpointTopology checkpointTopology =
                new DefaultCheckpointTopology(graph, DUMMY_EXCLUDE_BROADCASTED_VERTEX);

        assertNotNull(checkpointTopology.getCheckpointRegions());
        assertThat(checkpointTopology.getCheckpointRegions().size(), equalTo(1));
        assertNotNull(checkpointTopology.getExecutionAttempt2CheckpointRegion());
        assertThat(checkpointTopology.getExecutionAttempt2CheckpointRegion().size(), equalTo(2));
    }

    @Test
    public void testTwoAllToAllConnectedVerticesWithParallelismTwo() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        ExecutionGraph graph = createExecutionGraphWithTwoAllToAllConnectedVerticesAndEnableCheckpointing(2, counter, store);

        DefaultCheckpointTopology checkpointTopology =
                new DefaultCheckpointTopology(graph, DUMMY_EXCLUDE_BROADCASTED_VERTEX);

        assertNotNull(checkpointTopology.getCheckpointRegions());
        assertThat(checkpointTopology.getCheckpointRegions().size(), equalTo(1));
        assertNotNull(checkpointTopology.getExecutionAttempt2CheckpointRegion());
        assertThat(checkpointTopology.getExecutionAttempt2CheckpointRegion().size(), equalTo(4));
    }

    @Test
    public void testForwardConnectedVerticesWithParallelismOne() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        ExecutionGraph graph = RegionalCheckpointTestingUtils.createExecutionGraphWithForwardConnectedVerticesAndEnableCheckpointing(1, counter, store);

        DefaultCheckpointTopology checkpointTopology =
                new DefaultCheckpointTopology(graph, DUMMY_EXCLUDE_BROADCASTED_VERTEX);

        assertNotNull(checkpointTopology.getCheckpointRegions());
        assertThat(checkpointTopology.getCheckpointRegions().size(), equalTo(1));
        assertNotNull(checkpointTopology.getExecutionAttempt2CheckpointRegion());
        assertThat(checkpointTopology.getExecutionAttempt2CheckpointRegion().size(), equalTo(2));
    }

    @Test
    public void testForwardConnectedVerticesWithParallelismTwo() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        ExecutionGraph graph = RegionalCheckpointTestingUtils.createExecutionGraphWithForwardConnectedVerticesAndEnableCheckpointing(2, counter, store);

        DefaultCheckpointTopology checkpointTopology =
                new DefaultCheckpointTopology(graph, DUMMY_EXCLUDE_BROADCASTED_VERTEX);

        assertNotNull(checkpointTopology.getCheckpointRegions());
        assertThat(checkpointTopology.getCheckpointRegions().size(), equalTo(2));
        assertNotNull(checkpointTopology.getExecutionAttempt2CheckpointRegion());
        assertThat(checkpointTopology.getExecutionAttempt2CheckpointRegion().size(), equalTo(4));
    }

    @Test
    public void testRescaleConnectedVerticesWithParallelismTwoAndParallelismFour() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        ExecutionGraph graph =
                RegionalCheckpointTestingUtils.createExecutionGraphWithRescaleConnectedVerticesAndEnableCheckpointing(
                       2,
                       4,
                        counter,
                        store);

        DefaultCheckpointTopology checkpointTopology =
                new DefaultCheckpointTopology(graph, DUMMY_EXCLUDE_BROADCASTED_VERTEX);

        assertNotNull(checkpointTopology.getCheckpointRegions());
        assertThat(checkpointTopology.getCheckpointRegions().size(), equalTo(2));
        assertNotNull(checkpointTopology.getExecutionAttempt2CheckpointRegion());
        assertThat(checkpointTopology.getExecutionAttempt2CheckpointRegion().size(), equalTo(6));
    }

    @Test
    public void testBroadcastConnectedVerticesWithParallelismTwo() throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

        ExecutionGraph graph =
                RegionalCheckpointTestingUtils.createExecutionGraphWithBroadcastConnectedVerticesAndEnableCheckpointing(
                        2,
                        2,
                        counter,
                        store,
                        null);

        DefaultCheckpointTopology checkpointTopology =
                new DefaultCheckpointTopology(graph, true);

        assertNotNull(checkpointTopology.getCheckpointRegions());
        // 2 for the computing stream, and 2 for the broadcasted stream
        assertThat(checkpointTopology.getCheckpointRegions().size(), equalTo(4));
        assertNotNull(checkpointTopology.getExecutionAttempt2CheckpointRegion());
        assertThat(checkpointTopology.getExecutionAttempt2CheckpointRegion().size(), equalTo(4));
    }
}
