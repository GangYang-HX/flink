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

package org.apache.flink.core.fs;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Currently, this only used to execute delete operations with retry,
 * and may be replaced by speculative execution in the future.
 */
public class RetryProxy {

    private static final int DELETE_TIMEOUT_SECONDS = 3;

    private static final int DEFAULT_RETRY_TIMES = 2;

    private static final String RETRY_FAILURE_MESSAGE =
            "Failed to execute delete with " + DEFAULT_RETRY_TIMES + " attempts.";

    /**
     * Submit delete operation with retry.
     * @param runnable runnable
     * @return Future result.
     *        If delete is successful, it will return <code>true</code>.
     *        If delete is failed, it will return <code>false</code>.
     *        Otherwise, the future contains an exception.
     */
    public static CompletableFuture<Boolean> executeWithRetry(Runnable runnable) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        ExecutorHolder.getMonitorExecutorInstance().execute(() -> {
            try {
                for (int i = 1; ; i++) {
                    Future<?> future = ExecutorHolder.getIoExecutorInstance().submit(runnable);
                    try {
                        future.get(Math.max(DELETE_TIMEOUT_SECONDS / i, 1), TimeUnit.SECONDS);
                    } catch (TimeoutException ignored) {
                        future.cancel(true);
                        if (i == DEFAULT_RETRY_TIMES) {
                            throw new IOException(RETRY_FAILURE_MESSAGE);
                        }
                        continue;
                    }
                    result.complete(true);
                    break;
                }
            } catch (Throwable throwable) {
                if (throwable.getMessage() != null && throwable.getMessage().equals(RETRY_FAILURE_MESSAGE)) {
                    result.complete(false);
                } else {
                    result.completeExceptionally(throwable);
                }
            }
        });
        return result;
    }

    /**
     * Retry executors
     */
    private static class ExecutorHolder {
        /**
         * The executor used to execute actual delete operations.
         */
        static final ExecutorService IO_EXECUTOR_INSTANCE =
                Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

        /**
         * The executor used to monitor delete operations.
         */
        static final ExecutorService MONITOR_EXECUTOR_INSTANCE =
                Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

        private static ExecutorService getIoExecutorInstance() {
            return ExecutorHolder.IO_EXECUTOR_INSTANCE;
        }

        private static ExecutorService getMonitorExecutorInstance() {
            return ExecutorHolder.MONITOR_EXECUTOR_INSTANCE;
        }
    }
}
