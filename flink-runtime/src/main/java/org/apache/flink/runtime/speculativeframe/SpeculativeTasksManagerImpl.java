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

package org.apache.flink.runtime.speculativeframe;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.QueryServiceMode;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.speculativeframe.rpc.NeedReportToCoordinatorProperties;
import org.apache.flink.runtime.speculativeframe.rpc.SpeculativeTaskTimeCosts;
import org.apache.flink.runtime.speculativeframe.rpc.SpeculativeTasksOptions;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.speculativeframe.Constants.DEFAULT_MIN_GAP_THRESHOLDS;
import static org.apache.flink.runtime.speculativeframe.Constants.DEFAULT_MIN_NUM_DATA_POINTS;
import static org.apache.flink.runtime.speculativeframe.Constants.DEFAULT_MIN_THRESHOLDS;
import static org.apache.flink.runtime.speculativeframe.Constants.DEFAULT_SPECULATIVE_SCOPE;
import static org.apache.flink.runtime.speculativeframe.Constants.DEFAULT_THRESHOLD_TYPE;
import static org.apache.flink.runtime.speculativeframe.Constants.HISTOGRAM_WINDOW_SIZE;
import static org.apache.flink.runtime.speculativeframe.Constants.NUM_OF_TASKS_SPECULATED;
import static org.apache.flink.runtime.speculativeframe.Constants.SPECULATIVE_METRIC_GROUP;
import static org.apache.flink.runtime.speculativeframe.Constants.SPECULATIVE_TASK_TYPE;
import static org.apache.flink.runtime.speculativeframe.Constants.SPECULATIVE_THRESHOLD_GAUGE;
import static org.apache.flink.runtime.speculativeframe.Constants.TOTAL_NUM_OF_TASKS;

public class SpeculativeTasksManagerImpl implements SpeculativeTasksManager {

    private static final Logger LOG = LoggerFactory.getLogger(SpeculativeTasksManagerImpl.class);

    private static volatile SpeculativeTasksManagerImpl INSTANCE;

    /**
     * {@link JobMasterGateway} to communicate with the job master (actually
     * {@link SpeculativeTasksCoordinator}).
     */
    private final JobMasterGateway jobMaster;

    /**
     *  The taskmanager id
     */
    private final String taskmanagerId;

    /**
     * Executor service used by tasks whose executor service is not specified.
     */
    private final ExecutorService defaultExecutor;

    /**
     * Scheduled executor used to execute the scanning thread.
     */
    private final ScheduledExecutorService scheduler;

    /**
     * This will be scanned by the scanning thread.
     * NOTE: this may be not efficient enough. There's a chance that this data structure will be
     * replaced with another data structure or other data structures.
     */
    private final DoubleLinkedList<DoubleLinkedListNode<SpeculativeTaskInfo>> allTasks;

    /**
     * Executor services that are used to execute the submitted tasks.
     */
    private final Map<String, ExecutorService> taskExecutors;

    /**
     * Minimum number of data points collected before speculation takes effect.
     */
    private final Map<String, Integer> minNumDataPoints;

    /**
     * Scope for tasks.
     */
    private final Map<String, Tuple2<SpeculativeScope, Boolean>> scopes;

    /**
     * Threshold types.
     */
    private final Map<String, SpeculativeThresholdType> thresholdTypes;

    /**
     * Minimum thresholds in millisecond. If the threshold got from the statistics is less
     * than the minimum, the minimum will be used as the threshold. This can help avoid too
     * many unnecessary speculative tasks.
     */
    private final Map<String, Long> customMinThresholds;


    /**
     * Minimum gap between two reporting thresholds.
     */
    private final Map<String, Long> customMinGapThresholds;

    /**
     * Current number of data points. This will only be updated for TM-scoped tasks.
     */
    private final ConcurrentMap<String, Integer> currentNumDataPoints;

    /**
     * Statistics for TM-scoped tasks. JM-scoped tasks' statistics will be maintained
     * by {@link SpeculativeTasksCoordinator} on the job manager.
     */
    private final ConcurrentMap<String, Histogram> statistics;

    /**
     * Whether enough data points have been collected for tasks. This will be updated by
     * the RPC for JM-scoped tasks, and be updated by the {@link SpeculativeTasksManagerImpl}
     * itself for TM-scoped tasks.
     */
    private final ConcurrentMap<String, Boolean> enoughDataPoints;

    /**
     * This will be updated by the RPC (if scope is JM), or by the
     * {@link SpeculativeTasksManagerImpl} itself (if scope is TM). And this
     * will be accessed by the scanning thread to see whether a task should
     * be speculated.
     */
    private final ConcurrentHashMap<String, Long> currentThresholds;

    /**
     * The minimum threshold of all the task types. We maintain this field so that we can skip
     * some tasks when scanning all the tasks for speculative tasks. The idea is to skip the
     * scanning after the first task whose submitting time + globalMinimumThreshold is less than
     * current time.
     */
    private final AtomicLong globalMinimumThreshold = new AtomicLong(Integer.MAX_VALUE);

    /**
     * How many data points to cache before reporting to {@link SpeculativeTasksCoordinator}.
     */
    private final long dataCacheSize;

    /**
     * If not enough data points collected before this timeout, the cached data points will
     * be reported.
     */
    private final long dataCacheTimeout;

    /**
     * The interval of runing {@link TasksScanner}
     */
    private final long scanningInterval;

    /**
     * Cache data for JobManager-scoped tasks. This is an optimization to reduce number of RPC
     * calls between the task manager and job manager.
     */
    private final AtomicReference<ConcurrentMap<String, Collection<Long>>> dataCache;

    /**
     * Number of data cached in {@link #dataCache}.
     */
    private final AtomicInteger currentNumCachedDataPoints = new AtomicInteger(0);

    /**
     * Timestamp when the latest report happened.
     */
    private final AtomicLong lastReportTime = new AtomicLong(-1);

    /**
     * To prevent concurrent exceptions during traversal,
     * use this lock to ensure that tasks are submitted to the {@link #allTasks}
     */
    private final Object lock = new Object();

    @GuardedBy("lock")
    private volatile boolean isRunning = false;

    @GuardedBy("lock")
    private final Queue<SpeculativeTaskInfo> cachedTasks;

    /**
     * Used to register speculative task metrics.
     */
    private final MetricGroup metricGroup;

    /**
     * Counter for total task of task type.
     */
    private final Map<String, Counter> totalNumOfTasksCounters;

    /**
     * Counter for speculated task of task type.
     */
    private final Map<String, Counter> numOfTasksSpeculatedCounters;

    /**
     * Final error, may be null if exit normally.
     */
    @Nullable
    private Throwable error;

    public SpeculativeTasksManagerImpl(
            final JobMasterGateway jobMaster,
            final String taskmanagerId,
            final int schedulerPoolSize,
            final long scanningInterval,
            final long dataCacheSize,
            final long dataCacheTimeout,
            final MetricGroup taskManagerMetricGroup) {
        this.defaultExecutor = Executors.newSingleThreadExecutor();
        this.scheduler = Executors.newScheduledThreadPool(
                schedulerPoolSize, new ExecutorThreadFactory("Speculative manager scheduler"));
        this.jobMaster = jobMaster;
        this.taskmanagerId = taskmanagerId;

        this.allTasks = new DoubleLinkedList<>();

        this.taskExecutors = new ConcurrentHashMap<>();
        this.minNumDataPoints = new ConcurrentHashMap<>();
        this.thresholdTypes = new ConcurrentHashMap<>();
        this.scopes = new ConcurrentHashMap<>();
        this.customMinThresholds = new ConcurrentHashMap<>();
        this.customMinGapThresholds = new ConcurrentHashMap<>();

        this.currentNumDataPoints = new ConcurrentHashMap<>();
        this.statistics = new ConcurrentHashMap<>();
        this.enoughDataPoints = new ConcurrentHashMap<>();
        this.currentThresholds = new ConcurrentHashMap<>();

        this.dataCache = new AtomicReference<>(new ConcurrentHashMap<>());
        this.dataCacheSize = dataCacheSize;
        this.dataCacheTimeout = dataCacheTimeout;
        this.scanningInterval = scanningInterval;
        this.cachedTasks = new LinkedList<>();
        this.metricGroup = taskManagerMetricGroup.addGroup(SPECULATIVE_METRIC_GROUP, QueryServiceMode.DISABLED);
        this.totalNumOfTasksCounters = new HashMap<>();
        this.numOfTasksSpeculatedCounters = new HashMap<>();
    }

    public Map<String, ExecutorService> getTaskExecutors() {
        return taskExecutors;
    }

    @VisibleForTesting
    JobMasterGateway getJobMaster() {
        return jobMaster;
    }

    @VisibleForTesting
    ConcurrentHashMap<String, Long> getCurrentThresholds() {
        return currentThresholds;
    }

    @Override
    public SpeculativeTasksManager registerTaskType(SpeculativeProperties properties) {
        String taskType = properties.getSpeculativeTaskType();

        synchronized (lock) {
            if (scopes.containsKey(taskType)) {
                // other task already register it.
                return this;
            }

            // register task properties
            SpeculativeScope scope = properties.getScope().orElse(DEFAULT_SPECULATIVE_SCOPE);
            scopes.put(taskType, Tuple2.of(scope, scope == SpeculativeScope.TaskManager));
            taskExecutors.put(taskType, properties.getExecutor().orElse(defaultExecutor));
            minNumDataPoints.put(taskType, properties.getMinNumDataPoints().orElse(DEFAULT_MIN_NUM_DATA_POINTS));
            thresholdTypes.put(taskType, properties.getThresholdType().orElse(DEFAULT_THRESHOLD_TYPE));
            customMinThresholds.put(taskType, properties.getMinThreshold().orElse(DEFAULT_MIN_THRESHOLDS));
            customMinGapThresholds.put(taskType, properties.getMinGapBetweenThresholdReports()
                    .orElse(DEFAULT_MIN_GAP_THRESHOLDS));

            // register metric
            MetricGroup typeGroup = this.metricGroup.addGroup(SPECULATIVE_TASK_TYPE, taskType);
            typeGroup.gauge(SPECULATIVE_THRESHOLD_GAUGE, new ThresholdGauge(taskType));
            totalNumOfTasksCounters.put(taskType, typeGroup.counter(TOTAL_NUM_OF_TASKS));
            numOfTasksSpeculatedCounters.put(taskType, typeGroup.counter(NUM_OF_TASKS_SPECULATED));

            LOG.info("register task type {}", taskType);
            return this;
        }

    }

    @Override
    public void start() {

        if (isRunning) {
            return;
        }

        scheduler.scheduleWithFixedDelay(
                new TasksScanner(),
                scanningInterval,
                scanningInterval,
                TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(
                new CachedDataFlusher(),
                this.dataCacheTimeout,
                this.dataCacheTimeout,
                TimeUnit.MILLISECONDS);

        isRunning = true;

        LOG.info("Speculative Task Manager has been started!");
    }

    @Override
    public void submitTask(SpeculativeTaskInfo taskInfo) {
        preCheckGlobalState();

        if (!scopes.containsKey(taskInfo.getTaskType())) {
            throw new IllegalStateException(
                    "Registration is required before submitting tasks " + taskInfo.getTaskType());
        }

        synchronized (lock) {
            cachedTasks.add(taskInfo);
        }

        ExecutorService taskExclusiveExecutor = taskExecutors.get(taskInfo.getTaskType());
        SpeculativeTaskInfo.OriginalTask originalTask = taskInfo.new OriginalTask(this);
        originalTask.setTaskFuture(taskExclusiveExecutor.submit(originalTask));

        taskInfo.setOriginalTask(originalTask);

        inc(totalNumOfTasksCounters.get(taskInfo.getTaskType()));
    }

    @Override
    public void notifyThreshold(String taskType, long threshold) {
        preCheckGlobalState();
        LOG.debug("notify {} threshold is {}ms", taskType, threshold);
        if (!enoughDataPoints.containsKey(taskType)) {
            enoughDataPoints.put(taskType, true);
        }

        // The user-defined minimum threshold takes precedence over statistical values.
        currentThresholds.put(taskType, Math.max(threshold, customMinThresholds.get(taskType)));

        // save a global minimum threshold, which can speed up traversal.
        globalMinimumThreshold.set(Math.min(threshold, globalMinimumThreshold.get()));
    }

    @Override
    public void shutdown() {
        if (isRunning || error != null) {
            defaultExecutor.shutdown();
            scheduler.shutdown();
            isRunning = false;
            // It needs to be recreated while reusing task manager.
            INSTANCE = null;
            LOG.info("Shutdown speculative task manager!", error);
        }
    }


    /**
     * Traverse all tasks and check threshold.
     * TODO: In the future, the time wheel algorithm may be used to replace traversing all tasks.
     */
    public class TasksScanner implements Runnable {

        @Override
        public void run() {

            // put cache tasks
            // NOTE: We must put this step first, otherwise if enoughDataPoints is empty (or other condition is true),
            //       the cached task cannot be put into the allTasks.
            if (!cachedTasks.isEmpty()) {
                synchronized(lock) {
                    cachedTasks.forEach(task -> allTasks.addLast(new DoubleLinkedListNode<>(task)));
                    cachedTasks.clear();
                }
            }

            if (enoughDataPoints.isEmpty()) {
                return;
            }

            Iterator<DoubleLinkedListNode<SpeculativeTaskInfo>> iter = allTasks.iterator();
            long currentTime = System.currentTimeMillis();
            try {
                while (iter.hasNext()) {
                    SpeculativeTaskInfo taskInfo = iter.next().getPayload();
                    if (taskInfo.isFinished() || taskInfo.isSpeculatedTaskSubmitted()
                            || taskInfo.isOriginalTaskSucceeded()) {
                        iter.remove();
                        continue;
                    }

                    // The executor may be busy and the task has not yet been executed.
                    if (taskInfo.getSubmittedTime() <= 0) {
                        continue;
                    }

                    long submittedTime = taskInfo.getSubmittedTime();
                    if (currentTime < submittedTime + globalMinimumThreshold.get()) {
                        // If the current task does not meet the minimum time condition,
                        // we can skip this traversal directly.
                        break;
                    }

                    String taskType = taskInfo.getTaskType();
                    if (!enoughDataPoints.containsKey(taskType) || !enoughDataPoints.get(taskType)) {
                        continue;
                    }

                    if (!currentThresholds.containsKey(taskType)) {
                        continue;
                    }
                    if (submittedTime + currentThresholds.get(taskType) > currentTime) {
                        continue;
                    }

                    LOG.debug("Prepare to submit speculative task {}, submit time: {}, threshold: {}",
                            taskInfo.getTaskType(),
                            taskInfo.getSubmittedTime(),
                            currentThresholds.get(taskType));
                    // This task should be speculated.
                    taskInfo.getOriginalTask().onTimeout();
                    inc(numOfTasksSpeculatedCounters.get(taskType));
                }

                LOG.debug("The number of tasks contained in the scanner queue: {}", allTasks.size());
            } catch (Exception e) {
                // Internal error
                dispose(e);
            }

        }
    }

    public class CachedDataFlusher
            implements Runnable {
        @Override
        public void run() {
            synchronized (dataCache) {
                if (dataCache.get().isEmpty() ||
                        System.currentTimeMillis() - lastReportTime.get() < dataCacheTimeout) {
                    return;
                }

                ConcurrentMap<String, Collection<Long>> cachedData = dataCache.get();

                // Whether to report options
                reportOptionsIfNecessary(cachedData.keySet());

                List<Tuple2<String, Collection<Long>>> dataToReport = cachedData.entrySet().stream()
                        .map(ent -> Tuple2.of(ent.getKey(), ent.getValue()))
                        .collect(Collectors.toList());

                reportToJobMaster(dataToReport);

                dataCache.set(new ConcurrentHashMap<>());
                lastReportTime.set(System.currentTimeMillis());
            }
        }
    }

    private void preCheckGlobalState() {
        if (!isRunning || error != null) {
            throw new RuntimeException(error);
        }
    }

    private synchronized void inc(Counter counter) {
        counter.inc();
    }

    /**
     * Report the time cost for the given task type to {@link SpeculativeTasksCoordinator} if it
     * is scoped by job manager, or just add to the histogram for the given task type.
     *
     * @param taskType identifying the type of the task.
     * @param timeCost time spent to execute the task.
     */
    public void reportTimeCost(String taskType, long timeCost) {
        if (scopes.containsKey(taskType)) {
            if (scopes.get(taskType).f0 == SpeculativeScope.JobManager) {
                synchronized (dataCache) {
                    dataCache.get().computeIfAbsent(taskType, tt -> new ArrayList<>()).add(timeCost);
                    if (currentNumCachedDataPoints.incrementAndGet() >= dataCacheSize) {
                        ConcurrentMap<String, Collection<Long>> cachedDataPoints = dataCache.get();
                        // reset
                        dataCache.set(new ConcurrentHashMap<>());
                        currentNumCachedDataPoints.set(0);

                        // Whether to report options
                        reportOptionsIfNecessary(cachedDataPoints.keySet());

                        List<Tuple2<String, Collection<Long>>> dataToReport =
                                cachedDataPoints.entrySet().stream()
                                        .map(ent -> Tuple2.of(ent.getKey(), ent.getValue()))
                                        .collect(Collectors.toList());
                        // Accumulate enough number of data, or wait for enough time
                        reportToJobMaster(dataToReport);
                        lastReportTime.set(System.currentTimeMillis());
                    }
                }
            } else {
                statistics.computeIfAbsent(
                                taskType,
                                tt -> new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE))
                        .update(timeCost);

                // update immediately
                if (enoughDataPoints.containsKey(taskType) && enoughDataPoints.get(taskType)) {
                    notifyThreshold(taskType,
                                    (long) statistics.get(taskType)
                                            .getStatistics()
                                            .getQuantile(thresholdTypes.get(taskType).getQuantile()));
                    return;
                }

                int newNumDataPoints = currentNumDataPoints.computeIfAbsent(taskType, tt -> 0) + 1;
                currentNumDataPoints.put(taskType, newNumDataPoints);
                if ((!enoughDataPoints.containsKey(taskType) ||
                        !enoughDataPoints.get(taskType)) &&
                        newNumDataPoints >= minNumDataPoints.get(taskType)) {
                    enoughDataPoints.put(taskType, true);
                    // it will never be used.
                    currentNumDataPoints.remove(taskType);
                }
            }
        }
    }

    /**
     * Report the user-configured options for {@link SpeculativeScope#JobManager} tasks to
     * {@link SpeculativeTasksCoordinator}.
     * @return
     */
    private void reportOptionsIfNecessary(Collection<String> taskTypes) {
        Set<Tuple2<String, NeedReportToCoordinatorProperties>> options = taskTypes.stream()
                .filter(taskType -> !scopes.get(taskType).f1)
                .map(taskType -> {
                    NeedReportToCoordinatorProperties properties = NeedReportToCoordinatorProperties.of(
                            minNumDataPoints.get(taskType),
                            thresholdTypes.get(taskType),
                            customMinGapThresholds.get(taskType));
                    return Tuple2.of(taskType, properties);
                }).collect(Collectors.toSet());
        try {
            jobMaster.reportSpeculativeTaskOptions(
                    new SpeculativeTasksOptions(options, taskmanagerId));
        } catch (Exception e) {
            String msg = String.format("report options to JobMaster %s error", jobMaster.getAddress());
            LOG.error(msg, e);
            dispose(e);
        }
    }


    private void dispose(Throwable throwable) {
        LOG.error("Dispose speculative task manager because of", throwable);
        isRunning = false;
        if (error == null) {
            error = throwable;
        }
    }


    private void reportToJobMaster(Collection<Tuple2<String, Collection<Long>>> dataToReport) {
        CompletableFuture.supplyAsync(() ->
                jobMaster.reportSpeculativeTaskTimeCost(
                        new SpeculativeTaskTimeCosts(dataToReport)), scheduler)
                .thenCompose(Function.identity())
                .whenCompleteAsync((unused, throwable) -> {
                    // TODO: Retry should be implemented in the future.
                    if (throwable != null) {
                        LOG.warn("report data to JobMaster {} error", jobMaster.getAddress(), throwable);
                    }
                });
    }

    /**
     * Get or create a singleton SpeculativeTasksManager.
     * NOTE: There is only one SpeculativeTasksManager on a taskmanager.
     * @param jobMaster jobMaster
     * @param configuration configuration
     * @return a SpeculativeTasksManagerImpl instance
     */
    public static SpeculativeTasksManagerImpl getOrCreateSpeculativeTasksManager(
            JobMasterGateway jobMaster,
            Configuration configuration,
            String taskmanagerId,
            MetricGroup taskManagerMetricGroup) {

        Preconditions.checkNotNull(jobMaster);
        Preconditions.checkNotNull(configuration);

        if (INSTANCE == null) {
            synchronized (SpeculativeTasksManagerImpl.class) {
                // double check
                if (INSTANCE == null) {
                    INSTANCE = new SpeculativeTasksManagerImpl(
                            jobMaster,
                            taskmanagerId,
                            configuration.get(ExecutionOptions.SPECULATIVE_THREAD_POOL_SIZE),
                            configuration.get(ExecutionOptions.SPECULATIVE_SCAN_INTERVAL),
                            configuration.get(ExecutionOptions.SPECULATIVE_REPORT_BATCH_SIZE),
                            configuration.get(ExecutionOptions.SPECULATIVE_REPORTING_TIMEOUT),
                            taskManagerMetricGroup);
                    return INSTANCE;
                }
            }
        }
        return INSTANCE;
    }


    private class ThresholdGauge implements Gauge<Long> {

        private final String taskType;

        public ThresholdGauge(String taskType) {
            this.taskType = taskType;
        }

        @Override
        public Long getValue() {
            Long threshold = SpeculativeTasksManagerImpl.this.currentThresholds.get(taskType);
            return threshold == null ? 0L : threshold;
        }
    }
}
