package org.apache.flink.bili.external.excalibur.dispatcher;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.bili.external.excalibur.configuration.ExcaliburConfigOptions;
import org.apache.flink.bili.external.excalibur.consts.MetadataConst;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrapFactory;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rpc.RpcService;

import com.bilibili.flink.excalibur.metastore.metastore.RedisMetastoreUtil;
import com.bilibili.flink.excalibur.metastore.pojo.JobMeta;
import com.bilibili.flink.excalibur.metastore.pojo.SessionClusterMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author shijingqi
 * @date 2022/1/14
 */
public class MetadataStandaloneDispatcher extends StandaloneDispatcher {
    private final DispatcherServices dispatcherServices;
    private final Configuration configuration;
    private final RedisMetastoreUtil redisMetastoreUtil;

    private final Time timeout;
    private long startTime;
    private long updateTime;

    public MetadataStandaloneDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobResults,
                dispatcherBootstrapFactory,
                dispatcherServices);
        this.dispatcherServices = dispatcherServices;
        this.configuration = dispatcherServices.getConfiguration();

        // init metastore
        Map<String, String> params = new HashMap<>(4);
        params.put(
                MetadataConst.COMMON_PREFIX + MetadataConst.SERVER_ENV,
                configuration.getString(
                        ConfigOptions.key(MetadataConst.SERVER_ENV)
                                .stringType()
                                .defaultValue(MetadataConst.DEFAULT_SERVER_ENV)
                                .withDescription("")));
        params.put(
                MetadataConst.COMMON_PREFIX + MetadataConst.REDIS_ADDRESS,
                configuration.getString(
                        ConfigOptions.key(MetadataConst.REDIS_ADDRESS)
                                .stringType()
                                .noDefaultValue()
                                .withDescription("")));
        this.redisMetastoreUtil = new RedisMetastoreUtil(params);

        this.timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));
    }

    @Override
    public void onStart() throws Exception {
        super.onStart();

        startTime = System.currentTimeMillis();
        this.getRpcService()
                .getScheduledExecutor()
                .scheduleAtFixedRate(
                        reportMetadata(),
                        0,
                        configuration.getLong(ExcaliburConfigOptions.METADATA_REPORT_FREQUENCY),
                        TimeUnit.SECONDS);
    }

    private Runnable reportMetadata() {
        return () -> {
            try {
                updateTime = System.currentTimeMillis();
                log.info("report metadata at {}", updateTime);

                // 1. session cluster metadata
                SessionClusterMeta.Builder sessionClusterMetaBuilder =
                        SessionClusterMeta.newBuilder();
                String yarnCluster = configuration.getString(ExcaliburConfigOptions.YARN_CLUSTER);
                String sessionId = configuration.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
                String sessionKey = String.format("%s_%s", sessionId, yarnCluster);
                sessionClusterMetaBuilder.setYarnCluster(yarnCluster);
                sessionClusterMetaBuilder.setSessionId(sessionId);
                sessionClusterMetaBuilder.setSessionKey(sessionKey);
                sessionClusterMetaBuilder.setSessionState("RUNNING");
                sessionClusterMetaBuilder.setFlinkVersion(
                        configuration.getString(ExcaliburConfigOptions.FLINK_VERSION));
                sessionClusterMetaBuilder.setQueue(
                        configuration.getString(ExcaliburConfigOptions.APPLICATION_QUEUE));
                sessionClusterMetaBuilder.setPriority(
                        configuration.getString(ExcaliburConfigOptions.PRIORITY));
                sessionClusterMetaBuilder.setAppName(
                        configuration.getString(ExcaliburConfigOptions.APPLICATION_NAME));
                sessionClusterMetaBuilder.setExtraConfig(
                        configuration.getString(
                                ExcaliburConfigOptions.SESSION_CUSTOM_EXTRA_CONFIG));

                // containers_vcores、tm_memory、tm_num_slots
                sessionClusterMetaBuilder.setTmNumSlots(
                        configuration.get(TaskManagerOptions.NUM_TASK_SLOTS));
                TaskExecutorProcessSpec taskExecutorProcessSpec =
                        TaskExecutorProcessUtils.processSpecFromConfig(configuration);
                double cores =
                        TaskExecutorProcessUtils.getCpuCoresWithFallbackConfigOption(
                                configuration, ExcaliburConfigOptions.VCORES);
                sessionClusterMetaBuilder.setContainersVcores((int) cores);
                MemorySize totalProcessMem =
                        configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
                sessionClusterMetaBuilder.setTmMemory(
                        totalProcessMem == null ? -1 : totalProcessMem.getMebiBytes());
                sessionClusterMetaBuilder.setTmTaskHeapSize(
                        taskExecutorProcessSpec.getTaskHeapSize().getMebiBytes());
                sessionClusterMetaBuilder.setTmTaskOffHeapSize(
                        taskExecutorProcessSpec.getTaskOffHeapSize().getMebiBytes());
                sessionClusterMetaBuilder.setTmNetworkMemorySize(
                        taskExecutorProcessSpec.getNetworkMemSize().getMebiBytes());
                sessionClusterMetaBuilder.setTmManagedMemorySize(
                        taskExecutorProcessSpec.getManagedMemorySize().getMebiBytes());

                sessionClusterMetaBuilder.setStartTime(startTime);
                sessionClusterMetaBuilder.setUpdateTime(updateTime);
                sessionClusterMetaBuilder.setStopTime(-1L);

                // from CompletableFuture: TaskManager num、job id list
                // same as runResourceManagerCommand()
                CompletableFuture<ResourceOverview> taskManagerOverviewFuture =
                        dispatcherServices
                                .getResourceManagerGatewayRetriever()
                                .getFuture()
                                .thenApply(
                                        resourceManagerGateway ->
                                                resourceManagerGateway.requestResourceOverview(
                                                        timeout))
                                .thenCompose(Function.identity());
                taskManagerOverviewFuture.thenAcceptBoth(
                        requestMultipleJobDetails(timeout),
                        (resourceOverview, multipleJobsDetails) -> {
                            sessionClusterMetaBuilder.setTmNum(
                                    resourceOverview.getNumberTaskManagers());

                            List<String> jobIdList = new ArrayList<>();
                            List<String> completedJobIdList = new ArrayList<>();

                            multipleJobsDetails
                                    .getJobs()
                                    .forEach(
                                            jobDetails -> {
                                                // 2. job metadata
                                                JobMeta.Builder jobMetaBuilder =
                                                        JobMeta.newBuilder();
                                                // session id
                                                jobMetaBuilder.setSessionKey(sessionKey);
                                                // 任务并行度
                                                // jobMetaBuilder.setParallelism(configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM));

                                                JobID jobId = jobDetails.getJobId();
                                                String jobIdStr = jobId.toString();
                                                jobMetaBuilder.setJobId(jobIdStr);
                                                jobMetaBuilder.setJobName(jobDetails.getJobName());
                                                // job status
                                                String jobStatus =
                                                        jobDetails.getStatus().toString();
                                                jobMetaBuilder.setJobState(jobStatus);

                                                long jobUpdateTime;
                                                // job id list
                                                if (!jobDetails
                                                        .getStatus()
                                                        .isGloballyTerminalState()) {
                                                    jobIdList.add(jobIdStr);
                                                    jobUpdateTime =
                                                            Math.max(
                                                                    updateTime,
                                                                    jobDetails.getLastUpdateTime());
                                                } else {
                                                    completedJobIdList.add(jobIdStr);
                                                    jobUpdateTime = jobDetails.getLastUpdateTime();
                                                }

                                                // time
                                                jobMetaBuilder.setStartTime(
                                                        jobDetails.getStartTime());
                                                jobMetaBuilder.setUpdateTime(jobUpdateTime);
                                                jobMetaBuilder.setEndTime(jobDetails.getEndTime());

                                                requestJob(jobId, timeout)
                                                        .thenAccept(
                                                                archivedExecutionGraph -> {
                                                                    ArchivedExecutionConfig
                                                                            archivedExecutionConfig =
                                                                                    archivedExecutionGraph
                                                                                            .getArchivedExecutionConfig();
                                                                    if (archivedExecutionConfig
                                                                            != null) {
                                                                        // parallelism
                                                                        jobMetaBuilder
                                                                                .setParallelism(
                                                                                        archivedExecutionConfig
                                                                                                .getParallelism());
                                                                        Map<String, String>
                                                                                globalJobParameters =
                                                                                        archivedExecutionConfig
                                                                                                .getGlobalJobParameters();
                                                                        String jobExtraConfig =
                                                                                globalJobParameters
                                                                                        .getOrDefault(
                                                                                                ExcaliburConfigOptions
                                                                                                        .JOB_CUSTOM_EXTRA_CONFIG
                                                                                                        .key(),
                                                                                                "");
                                                                        jobMetaBuilder
                                                                                .setExtraConfig(
                                                                                        jobExtraConfig);
                                                                    } else {
                                                                        jobMetaBuilder
                                                                                .setParallelism(
                                                                                        configuration
                                                                                                .getInteger(
                                                                                                        CoreOptions
                                                                                                                .DEFAULT_PARALLELISM));
                                                                        jobMetaBuilder
                                                                                .setExtraConfig("");
                                                                    }

                                                                    try {
                                                                        JobMeta jobMeta =
                                                                                jobMetaBuilder
                                                                                        .build();
                                                                        log.info(
                                                                                "job meta: {}",
                                                                                jobMeta);
                                                                        redisMetastoreUtil
                                                                                .storeJobMeta(
                                                                                        jobMeta);
                                                                    } catch (Exception e) {
                                                                        log.error(
                                                                                "report job metadata failed.");
                                                                    }
                                                                });
                                            });

                            // running job and completed job
                            sessionClusterMetaBuilder.addAllJobId(jobIdList);
                            sessionClusterMetaBuilder.addAllCompletedJobId(completedJobIdList);

                            try {
                                SessionClusterMeta sessionClusterMeta =
                                        sessionClusterMetaBuilder.build();
                                log.info("session meta: {}", sessionClusterMeta);
                                redisMetastoreUtil.storeSessionCluster(sessionClusterMeta);
                            } catch (Exception e) {
                                log.error("report session metadata failed.");
                            }
                        });
            } catch (Exception e) {
                log.error("report metadata failed.", e);
            }
        };
    }
}
