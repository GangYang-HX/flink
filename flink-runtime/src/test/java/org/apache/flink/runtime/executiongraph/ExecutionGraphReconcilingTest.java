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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolImpl;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests the reconciling behaviour of the {@link ExecutionGraph}.
 */
public class ExecutionGraphReconcilingTest extends TestLogger {

	private static final int NUM_TASKS = 31;

	private static final JobID TEST_JOB_ID = new JobID();

	private static final ComponentMainThreadExecutor mainThreadExecutor =
		ComponentMainThreadExecutorServiceAdapter.forMainThread();

    private static final Time RECONCILE_TIMEOUT = Time.milliseconds(100);

	/**
	 * Reconciling from CREATED goes to RECONCILING directly.
	 */
	@Test
	public void testReconcilingFromCreated() throws Exception {
        ExecutionGraph eg = createExecutionGraph();

		assertEquals(JobStatus.CREATED, eg.getState());

		// reconcile
		eg.reconcile();

		assertEquals(JobStatus.RECONCILING, eg.getState());
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			assertEquals(ExecutionState.RECONCILING, ev.getCurrentExecutionAttempt().getState());
		}
	}

    /**
     * Only permit switching CREATED to RECONCILING, so Reconciling from RUNNING state will throw an
     * illegal state exception.
	 * Switching the other states to RECONCILING will be as same as this case.
     */
    @Test
    public void testReconcilingFromRunning() throws Exception {
		ExecutionGraph eg = createExecutionGraph();

		startAndScheduleExecutionGraph(eg);
		assertEquals(JobStatus.RUNNING, eg.getState());

		// reconcile
		eg.reconcile();

		assertEquals(IllegalStateException.class, eg.getFailureCause().getClass());
	}

	/**
	 * Test cancel job when job is in RECONCILING state.
	 */
	@Test
    public void testCancelFromReconciling() throws Exception {
		ExecutionGraph eg = createExecutionGraph();

		assertEquals(JobStatus.CREATED, eg.getState());

		// reconcile
		eg.reconcile();
		assertEquals(JobStatus.RECONCILING, eg.getState());

		eg.cancel();

		eg.waitUntilTerminal();
		assertEquals(JobStatus.CANCELED, eg.getState());
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			assertEquals(ExecutionState.CANCELED, ev.getCurrentExecutionAttempt().getState());
		}
	}

	@Test
	public void testReconcilingToRunning() throws Exception {
		ExecutionGraph eg = createExecutionGraph();
		assertEquals(JobStatus.CREATED, eg.getState());

		// reconcile
		eg.reconcile();
		assertEquals(JobStatus.RECONCILING, eg.getState());

		// switch all tasks to running
		for (ExecutionVertex vertex : eg.getVerticesTopologically().iterator().next().getTaskVertices()) {
			vertex.getCurrentExecutionAttempt().switchToRunning();
		}

		assertEquals(JobStatus.RUNNING, eg.getState());
	}

    @Test
    public void testReconcilingToFailed() throws Exception {
		ExecutionGraph eg = createExecutionGraph();
		assertEquals(JobStatus.CREATED, eg.getState());

		// reconcile
		eg.reconcile();
		assertEquals(JobStatus.RECONCILING, eg.getState());

        Exception testExcpetion = new Exception("test exception");
		eg.failJob(testExcpetion);

		// after fail job, JobStatus changed to FAILED while its tasks' state changed to CANCELED
		assertEquals(JobStatus.FAILED, eg.getState());
		assertEquals(testExcpetion, eg.getFailureCause());
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			assertEquals(ExecutionState.CANCELED, ev.getCurrentExecutionAttempt().getState());
		}
	}

    @Test
    public void testReconcilingToCanceled() throws Exception {
		ExecutionGraph eg = createExecutionGraph();
		assertEquals(JobStatus.CREATED, eg.getState());

		// reconcile
		eg.reconcile();
		assertEquals(JobStatus.RECONCILING, eg.getState());

		eg.cancel();

		assertEquals(JobStatus.CANCELED, eg.getState());
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			assertEquals(ExecutionState.CANCELED, ev.getCurrentExecutionAttempt().getState());
		}
	}

	@Test
	public void testReconcilingTimeout() throws Exception {
		ExecutionGraph eg = createExecutionGraph();
		assertEquals(JobStatus.CREATED, eg.getState());

		// reconcile
		eg.reconcile();
		assertEquals(JobStatus.RECONCILING, eg.getState());

		eg.waitUntilTerminal();
		assertEquals(JobStatus.FAILED, eg.getState());
		assertEquals(TimeoutException.class, eg.getFailureCause().getCause().getClass());
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static ExecutionGraph createExecutionGraph() throws Exception {
		ExecutionGraph eg;
		try (SlotPool slotPool = createSlotPoolImpl()) {
			eg = TestingExecutionGraphBuilder
				.newBuilder()
				.setJobGraph(createJobGraph())
				.setRestartStrategy(new InfiniteDelayRestartStrategy())
				.setSlotProvider(createSchedulerWithSlots(slotPool))
				.setReconcileTimeout(RECONCILE_TIMEOUT)
				.build();

		}
		return eg;
	}

	@Nonnull
	private static SlotPoolImpl createSlotPoolImpl() {
		return new TestingSlotPoolImpl(TEST_JOB_ID);
	}

	private static JobGraph createJobGraph() {
		JobVertex sender = ExecutionGraphTestUtils.createJobVertex("Task", NUM_TASKS, NoOpInvokable.class);
		return new JobGraph("Pointwise job", sender);
	}

	private static void startAndScheduleExecutionGraph(ExecutionGraph executionGraph) throws Exception {
		executionGraph.start(mainThreadExecutor);
		assertThat(executionGraph.getState(), is(JobStatus.CREATED));
		executionGraph.scheduleForExecution();
		assertThat(executionGraph.getState(), is(JobStatus.RUNNING));
	}

	private static Scheduler createSchedulerWithSlots(SlotPool slotPool) throws Exception {
		return createSchedulerWithSlots(slotPool, new LocalTaskManagerLocation());
	}

	private static Scheduler createSchedulerWithSlots(SlotPool slotPool, TaskManagerLocation taskManagerLocation) throws Exception {
		return createSchedulerWithSlots(slotPool, taskManagerLocation, NUM_TASKS);
	}

	private static Scheduler createSchedulerWithSlots(SlotPool slotPool, TaskManagerLocation taskManagerLocation, int numSlots) throws Exception {
		final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		setupSlotPool(slotPool);
		Scheduler scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		scheduler.start(mainThreadExecutor);
		slotPool.registerTaskManager(taskManagerLocation.getResourceID(), taskManagerLocation);

		final List<SlotOffer> slotOffers = new ArrayList<>(NUM_TASKS);
		for (int i = 0; i < numSlots; i++) {
			final AllocationID allocationId = new AllocationID();
			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.ANY);
			slotOffers.add(slotOffer);
		}

		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

		return scheduler;
	}

	private static void setupSlotPool(SlotPool slotPool) throws Exception {
		final String jobManagerAddress = "foobar";
		final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		slotPool.start(JobMasterId.generate(), jobManagerAddress, mainThreadExecutor);
		slotPool.connectToResourceManager(resourceManagerGateway);
	}

}
