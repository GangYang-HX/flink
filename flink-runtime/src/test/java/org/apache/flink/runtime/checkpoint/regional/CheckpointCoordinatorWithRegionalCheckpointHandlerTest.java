package org.apache.flink.runtime.checkpoint.regional;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils;
import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;
import org.apache.flink.runtime.checkpoint.CheckpointHandler;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionVertex;
import static org.apache.flink.runtime.checkpoint.regional.RegionalCheckpointTestingUtils.createExecutionGraphWithSingleVertexAndEnableCheckpointing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckpointCoordinatorWithRegionalCheckpointHandlerTest {

    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    private final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
    private final CheckpointIDCounter counter = new RegionalCheckpointTestingUtils.TestingCheckpointIDCounter(
            counterShutdownFuture);

    private final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
    private final CompletedCheckpointStore store = new RegionalCheckpointTestingUtils.TestingCompletedCheckpointStore(
            storeShutdownFuture);

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    public void testCheckpointHandlerType() throws Exception {

        ExecutionGraph graph = createExecutionGraphWithSingleVertexAndEnableCheckpointing(
                1,
                counter,
                store,
                manuallyTriggeredScheduledExecutor);

        CheckpointCoordinator coordinator = graph.getCheckpointCoordinator();

        assert coordinator != null;
        assertTrue(coordinator.getCheckpointHandler() instanceof RegionalCheckpointHandler);
    }

    @Test
    public void testTriggerAndDeclineCheckpointThenFailureManagerThrowsExceptionWhenNoSucceededCheckpoint()
            throws Exception {

        final JobID jid = new JobID();
        String errorMsg = "There is no succeeded checkpoint for region";
        CheckpointCoordinator coordinator = getCheckpointCoordinator(jid, errorMsg);
        coordinator.startCheckpointScheduler();

        try {
            // trigger the checkpoint. this should succeed
            final CompletableFuture<CompletedCheckpoint> checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            long checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
            PendingCheckpoint checkpoint = coordinator.getPendingCheckpoints().get(checkpointId);

            // acknowledge from one of the tasks
            coordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[0].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);
            assertFalse(checkpoint.isDiscarded());
            assertFalse(checkpoint.areTasksFullyAcknowledged());

            // decline checkpoint from the other task
            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[1].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            fail("Test failed");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals(e.getMessage(), errorMsg);
        } finally {
            coordinator.shutdown(JobStatus.FINISHED);
        }
    }

    @Test
    public void testTriggerAndDeclineCheckpointWhenThereIsSucceededCheckpoint()
            throws Exception {

        final JobID jid = new JobID();
        CheckpointCoordinator coordinator = getCheckpointCoordinator(jid, "Dummy");
        CheckpointHandler handler = coordinator.getCheckpointHandler();
        coordinator.startCheckpointScheduler();
        coordinator.setCheckpointHandler(handler);

        try {
            // trigger the checkpoint. this should succeed
            CompletableFuture<CompletedCheckpoint> checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            long checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            for (int i = 0; i < 3; i++) {
                // acknowledge from one of the tasks
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(
                                jid,
                                coordinator.getTasksToCommitTo()[i].getCurrentExecutionAttempt().getAttemptId(),
                                checkpointId,
                                new CheckpointMetrics(),
                                mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[i])),
                        TASK_MANAGER_LOCATION_INFO);
            }

            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            // decline checkpoint from the other task
            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[1].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);
        } catch (Exception e) {
            fail("Unexpected exception " + e.getMessage());
        } finally {
            coordinator.shutdown(JobStatus.FINISHED);
        }
    }

    @Test
    public void testTooManyDeclineMessages()
            throws Exception {

        final JobID jid = new JobID();
        String errorMsg = "Ratio of failed or expired regions reached max allowed value";
        CheckpointCoordinator coordinator = getCheckpointCoordinator(jid, errorMsg);
        coordinator.startCheckpointScheduler();

        try {
            // trigger the checkpoint. this should succeed
            CompletableFuture<CompletedCheckpoint> checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            long checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            for (int i = 0; i < 3; i++) {
                // acknowledge from one of the tasks
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(
                                jid,
                                coordinator.getTasksToCommitTo()[i].getCurrentExecutionAttempt().getAttemptId(),
                                checkpointId),
                        TASK_MANAGER_LOCATION_INFO);
            }

            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            // decline checkpoint from the other task
            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[0].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            // 2 of 3 declined messages makes the ratio greater than 0.5.
            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[1].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            fail("Test failed");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals(e.getMessage(), errorMsg);
        } finally {
            coordinator.shutdown(JobStatus.FINISHED);
        }
    }

    @Test
    public void testTooManySuccessiveDeclineMessages()
            throws Exception {

        final JobID jid = new JobID();
        String errorMsg = "Consecutive failures or expires of checkpoint region reached max allowed value";
        CheckpointCoordinator coordinator = getCheckpointCoordinator(jid, errorMsg);
        coordinator.startCheckpointScheduler();

        try {
            // trigger the checkpoint. this should succeed
            CompletableFuture<CompletedCheckpoint> checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            long checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            for (int i = 0; i < 3; i++) {
                // acknowledge from one of the tasks
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(
                                jid,
                                coordinator.getTasksToCommitTo()[i].getCurrentExecutionAttempt().getAttemptId(),
                                checkpointId),
                        TASK_MANAGER_LOCATION_INFO);
            }

            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            // decline checkpoint from the other task
            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[0].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            // decline checkpoint from the other task
            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[0].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            fail("Test failed");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals(e.getMessage(), errorMsg);
        } finally {
            coordinator.shutdown(JobStatus.FINISHED);
        }
    }

    @Test
    public void testExpiredCheckpointWithSucceededCheckpoint()
            throws Exception {

        final JobID jid = new JobID();
        String errorMsg = "Consecutive failures or expires of checkpoint region reached max allowed value";
        CheckpointCoordinator coordinator = getCheckpointCoordinator(jid, errorMsg);
        CheckpointHandler handler = coordinator.getCheckpointHandler();
        coordinator.startCheckpointScheduler();
        coordinator.setCheckpointHandler(handler);

        try {
            // trigger the checkpoint. this should succeed
            CompletableFuture<CompletedCheckpoint> checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            long checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            for (int i = 0; i < 3; i++) {
                // acknowledge from one of the tasks
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(
                                jid,
                                coordinator.getTasksToCommitTo()[i].getCurrentExecutionAttempt().getAttemptId(),
                                checkpointId,
                                new CheckpointMetrics(),
                                mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[i])),
                        TASK_MANAGER_LOCATION_INFO);
            }

            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            // decline checkpoint from the other task
            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[0].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            // decline checkpoint from the other task
            coordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[0].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[0])),
                    TASK_MANAGER_LOCATION_INFO);

            coordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[1].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[1])),
                    TASK_MANAGER_LOCATION_INFO);

            PendingCheckpoint pendingCheckpoint = coordinator.getPendingCheckpoints().get(checkpointId);
            CheckpointCoordinator.CheckpointCanceller canceller = pendingCheckpoint.getCanceller();
            // Manually simulate checkpoint expiration.
            canceller.run();

            pendingCheckpoint.getCancellerHandle().cancel(false);

            assertTrue(coordinator.getSuccessfulCheckpoints()
                    .stream()
                    .map(CompletedCheckpoint::getCheckpointID)
                    .collect(Collectors.toSet())
                    .contains(checkpointId));
        } catch (Exception e) {
            fail("Test failed");
        } finally {
            coordinator.shutdown(JobStatus.FINISHED);
        }
    }

    @Test
    public void testCheckpointRetain()
            throws Exception {

        final JobID jid = new JobID();
        String errorMsg = "Consecutive failures or expires of checkpoint region reached max allowed value";
        CheckpointCoordinator coordinator = getCheckpointCoordinator(jid, errorMsg);
        CheckpointHandler handler = coordinator.getCheckpointHandler();
        coordinator.startCheckpointScheduler();
        coordinator.setCheckpointHandler(handler);
        try {
            // trigger the checkpoint. this should succeed
            CompletableFuture<CompletedCheckpoint> checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            // The first checkpoint will succeed.
            long checkpointId = coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();

            for (int i = 0; i < 3; i++) {
                // acknowledge from one of the tasks
                TaskStateSnapshot snapshot = mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[i]);
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(
                                jid,
                                coordinator.getTasksToCommitTo()[i].getCurrentExecutionAttempt().getAttemptId(),
                                checkpointId,
                                new CheckpointMetrics(),
                                snapshot),
                        TASK_MANAGER_LOCATION_INFO);
            }

            // The first task of the second checkpoint will fail
            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId += 1;

            // decline checkpoint from the first task
            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[0].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            for (int i = 1; i < 3; i++) {
                // acknowledge from one of the tasks
                TaskStateSnapshot snapshot = mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[i]);
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(
                                jid,
                                coordinator.getTasksToCommitTo()[i].getCurrentExecutionAttempt().getAttemptId(),
                                checkpointId,
                                new CheckpointMetrics(),
                                snapshot),
                        TASK_MANAGER_LOCATION_INFO);
            }

            // The first checkpoint will not be deleted as it is referenced by the second checkpoint.
            assertEquals(2, coordinator.getCheckpointStore().getAllCheckpoints().size());

            assertTrue(coordinator.getCheckpointStore().getAllCheckpoints().get(0).isReferenced());

            // The second task of the third checkpoint will fail.
            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId += 1;

            // decline checkpoint from the other task
            coordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[0].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[0])),
                    TASK_MANAGER_LOCATION_INFO);

            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[1].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            coordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            coordinator.getTasksToCommitTo()[2].getCurrentExecutionAttempt().getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[2])),
                    TASK_MANAGER_LOCATION_INFO);


            // The second checkpoint will not be deleted as it is referenced by the second checkpoint.
            assertEquals(2, coordinator.getCheckpointStore().getAllCheckpoints().size());

            assertTrue(coordinator.getCheckpointStore().getAllCheckpoints().get(0).isReferenced());

            // trigger another checkpoint that will succeed.
            checkPointFuture = coordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkPointFuture.isCompletedExceptionally());

            checkpointId += 1;

            for (int i = 0; i < 3; i++) {
                // acknowledge from one of the tasks
                TaskStateSnapshot snapshot = mockTaskStateSnapshot(coordinator.getTasksToCommitTo()[i]);
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(
                                jid,
                                coordinator.getTasksToCommitTo()[i].getCurrentExecutionAttempt().getAttemptId(),
                                checkpointId,
                                new CheckpointMetrics(),
                                snapshot),
                        TASK_MANAGER_LOCATION_INFO);
            }

            // The other checkpoints are not referenced. So they will be cleaned.
            assertEquals(1, coordinator.getCheckpointStore().getAllCheckpoints().size());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test failed");
        } finally {
            coordinator.shutdown(JobStatus.FINISHED);
        }
    }

    private ExecutionVertex[] createExecutionVertices() throws JobException, JobExecutionException {
        // create some mock execution vertices and trigger some checkpoint
        final ExecutionAttemptID attempt1 = new ExecutionAttemptID();
        final ExecutionAttemptID attempt2 = new ExecutionAttemptID();
        final ExecutionAttemptID attempt3 = new ExecutionAttemptID();

        JobVertex jobVertex = new JobVertex("MockVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);
        jobVertex.setParallelism(3);

        final ExecutionGraph executionGraph = TestingExecutionGraphBuilder
                .newBuilder()
                .setJobGraph(new JobGraph(jobVertex))
                .build();

        ExecutionVertex vertex1 = mockExecutionVertex(attempt1);
        when(vertex1.getExecutionGraph()).thenReturn(executionGraph);
        ExecutionVertex vertex2 = mockExecutionVertex(attempt2);
        when(vertex2.getExecutionGraph()).thenReturn(executionGraph);
        ExecutionVertex vertex3 = mockExecutionVertex(attempt3);
        when(vertex3.getExecutionGraph()).thenReturn(executionGraph);

        return new ExecutionVertex[] {vertex1, vertex2, vertex3};
    }

    private RegionalCheckpointHandler createCheckpointHandler(JobID job, ExecutionVertex[] vertices) {
        CheckpointTopology topology = mock(CheckpointTopology.class);

        List<CheckpointRegion> regions = new ArrayList<>();
        for (ExecutionVertex vertex : vertices) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            CheckpointRegion triggerRegion = new DefaultCheckpointRegion();
            triggerRegion.add(attemptId, vertex);
            when(topology.getCheckpointRegion(attemptId)).thenReturn(triggerRegion);
            regions.add(triggerRegion);
        }

        when(topology.getCheckpointRegions()).thenReturn(regions);

        return new RegionalCheckpointHandler(
                job,
                topology,
                2,
                0.5);
    }

    private CheckpointCoordinator getCheckpointCoordinator(JobID job, String errorMsg) throws JobException, JobExecutionException {
        ExecutionVertex[] vertices = createExecutionVertices();
        RegionalCheckpointHandler handler = createCheckpointHandler(job, vertices);

        CheckpointFailureManager checkpointFailureManager = getCheckpointFailureManager(errorMsg);

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10) // periodic interval is 10 ms
                        .setCheckpointTimeout(200000) // timeout is very long (200 s)
                        .setMinPauseBetweenCheckpoints(0) // no extra delay
                        .setMaxConcurrentCheckpoints(2) // max two concurrent checkpoints
                        .setRegionalCheckpointEnabled(true) // enable regional checkpoint
                        .setMaxTolerableConsecutiveFailuresOrExpirations(2)
                        .setMaxTolerableFailureOrExpirationRatio(0.5)
                        .build();
        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                        .setJobId(job)
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setTasks(vertices)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(1))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointHandler(handler)
                        .setFailureManager(checkpointFailureManager)
                        .build();

        handler.setCheckpointCoordinator(coordinator);

        return coordinator;
    }

    private CheckpointFailureManager getCheckpointFailureManager(String errorMsg) {
        return new CheckpointFailureManager(
                0,
                new CheckpointFailureManager.FailJobCallback() {
                    @Override
                    public void failJob(Throwable cause) {
                        throw new RuntimeException(errorMsg);
                    }

                    @Override
                    public void failJobDueToTaskFailure(Throwable cause, ExecutionAttemptID failingTask) {
                        throw new RuntimeException(errorMsg);
                    }
                });
    }

    private TaskStateSnapshot mockTaskStateSnapshot(ExecutionVertex vertex) {
        TaskStateSnapshot snapshot = new TaskStateSnapshot();
        List<OperatorIDPair> operatorIDs = vertex.getJobVertex().getOperatorIDs();
        for (OperatorIDPair idPair: operatorIDs) {
            snapshot.putSubtaskStateByOperatorID(idPair.getGeneratedOperatorID(), mock(OperatorSubtaskState.class));
        }
        return snapshot;
    }
}
