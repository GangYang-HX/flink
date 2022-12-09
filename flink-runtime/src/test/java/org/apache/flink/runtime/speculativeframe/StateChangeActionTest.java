package org.apache.flink.runtime.speculativeframe;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StateChangeActionTest {

    SpeculativeTasksManagerImpl speculativeManager = mock(SpeculativeTasksManagerImpl.class);

    @Test
    public void testStateChangeAction() throws Exception {
        SpeculativeTaskInfo taskInfo = new SpeculativeTaskInfo.SpeculativeTaskInfoBuilder(
                "test",
                taskData -> null)
                .build();

        TestStateChangeAction originalAction = create(taskInfo, speculativeManager);
        TestStateChangeAction speculatedAction = create(taskInfo, speculativeManager);
        originalAction.setSpeculatedAction(speculatedAction);

        originalAction.run();
        originalAction.doneFuture.join();

        assertTrue(originalAction.submitFuture.getNow(false));
        assertTrue(originalAction.succeedFuture.getNow(false));
        assertFalse(originalAction.exceptionFuture.getNow(false));
        assertFalse(originalAction.duplicateFuture.getNow(false));

        assertTrue(speculatedAction.submitFuture.getNow(false));
        assertTrue(speculatedAction.duplicateFuture.getNow(false));
        assertFalse(speculatedAction.exceptionFuture.getNow(false));
        assertFalse(speculatedAction.succeedFuture.getNow(false));
    }

    @Test
    public void testStateChangeActionWithException() {
        SpeculativeTaskInfo taskInfo = new SpeculativeTaskInfo.SpeculativeTaskInfoBuilder(
                "test",
                taskData -> {
                    throw new RuntimeException("mock error");
                })
                .build();

        TestStateChangeAction originalAction = create(taskInfo, speculativeManager);
        CompletableFuture<Void> future = CompletableFuture.runAsync(originalAction);
        future.join();

        assertTrue(originalAction.submitFuture.getNow(false));
        assertTrue(originalAction.exceptionFuture.getNow(false));
        assertFalse(originalAction.duplicateFuture.getNow(false));
        assertFalse(originalAction.succeedFuture.getNow(false));
    }

    private TestStateChangeAction create(
            SpeculativeTaskInfo taskInfo,
            SpeculativeTasksManager speculativeTasksManager) {
        CompletableFuture<Boolean> submitFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> exceptionFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> succeedFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> duplicateFuture = new CompletableFuture<>();
        return new TestStateChangeAction(
                taskInfo,
                speculativeTasksManager,
                submitFuture,
                exceptionFuture,
                succeedFuture,
                duplicateFuture);
    }

    class TestStateChangeAction extends SpeculativeTaskInfo.CancellableTask {

        SpeculativeTaskInfo taskInfo;

        TestStateChangeAction speculatedAction;

        CompletableFuture<Boolean> submitFuture;

        CompletableFuture<Boolean> exceptionFuture;

        CompletableFuture<Boolean> succeedFuture;

        CompletableFuture<Boolean> duplicateFuture;

        CompletableFuture<Void> doneFuture;

        public TestStateChangeAction(SpeculativeTaskInfo taskInfo,
                                     SpeculativeTasksManager speculativeTasksManager,
                                     CompletableFuture<Boolean> submitFuture,
                                     CompletableFuture<Boolean> exceptionFuture,
                                     CompletableFuture<Boolean> succeedFuture,
                                     CompletableFuture<Boolean> duplicateFuture) {
            taskInfo.super(speculativeTasksManager);
            this.taskInfo = taskInfo;
            this.submitFuture = submitFuture;
            this.exceptionFuture = exceptionFuture;
            this.duplicateFuture = duplicateFuture;
            this.succeedFuture = succeedFuture;
        }

        public void setSpeculatedAction(TestStateChangeAction speculatedAction) {
            this.speculatedAction = speculatedAction;
        }

        @Override
        public void onSubmitted() {
            submitFuture.complete(true);
        }

        @Override
        public void onException(Exception exception) {
            exceptionFuture.complete(true);
        }

        @Override
        public void onTimeout() {
            doneFuture = CompletableFuture.runAsync(() -> {
                this.succeedFuture.join();
                speculatedAction.run();
            });
        }

        @Override
        public void onSucceeded(Object result) {
            speculatedAction.cancelled = true;
            succeedFuture.complete(true);
        }

        @Override
        public void onDuplicated(Object result) throws Exception {
            duplicateFuture.complete(true);
        }

        @Override
        public void run() {
            onSubmitted();
            try {
                taskInfo.getTask().apply(null);
                if (this.speculatedAction != null) {
                    onTimeout();
                }
                afterInvoke(null, 0);
            } catch (Exception e) {
                onException(e);
            }
        }

        @Override
        public Object beforeInvoke() {
            return null;
        }
    }
}
