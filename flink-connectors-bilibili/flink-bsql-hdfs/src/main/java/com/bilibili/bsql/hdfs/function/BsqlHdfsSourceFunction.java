package com.bilibili.bsql.hdfs.function;

import com.alibaba.fastjson.JSON;
import com.bilibili.bsql.hdfs.internal.FileFetcher;
import com.bilibili.bsql.hdfs.internal.FilePartition;
import com.bilibili.bsql.hdfs.internal.FileSourceProperties;
import com.bilibili.bsql.hdfs.internal.clean.CleanAggFunc;
import com.bilibili.bsql.hdfs.internal.clean.CleanInput;
import com.bilibili.bsql.hdfs.internal.clean.CleanOutput;
import com.bilibili.bsql.hdfs.internal.selector.FileSelector;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className BsqlHdfsSourceFunction.java
 * @description This is the description of BsqlHdfsSourceFunction.java
 * @createTime 2020-11-04 19:10:00
 */
public class BsqlHdfsSourceFunction<T> extends RichParallelSourceFunction<T> implements ResultTypeQueryable<T>, CheckpointedFunction {
    private final static Logger LOG = LoggerFactory.getLogger(BsqlHdfsSourceFunction.class);
    private final FileSourceProperties sourceProperties;
    private final FileSelector selector;
    private final DeserializationSchema<T> deserializationSchema;
    private List<FilePartition> allocatedFiles;
    private FileFetcher<T> fileFetcher;
    private transient ListState<FilePartition> fileOffset;
    private int partitionId;
    private int partitionNum;
    private GlobalAggregateManager aggregateManager;
    private ScheduledThreadPoolExecutor aggScheduler;

    public BsqlHdfsSourceFunction(FileSourceProperties sourceProperties, FileSelector selector, DeserializationSchema<T> deserializationSchema) {
        this.sourceProperties = sourceProperties;
        this.selector = selector;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        partitionId = getRuntimeContext().getIndexOfThisSubtask();
        partitionNum = getRuntimeContext().getNumberOfParallelSubtasks();
        if (allocatedFiles == null) {
            allocatedFiles = selector.selectAndSort(partitionId, partitionNum, sourceProperties);
        } else {
            LOG.info("use checkpoint state to recover job,index:{},fileOffset:{}", partitionId, JSON.toJSONString(allocatedFiles));
        }
        aggregateManager = ((StreamingRuntimeContext) getRuntimeContext()).getGlobalAggregateManager();
        aggScheduler = new ScheduledThreadPoolExecutor(1);
        aggScheduler.scheduleWithFixedDelay(this::updateAgg, 30, 120, TimeUnit.SECONDS);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        fileOffset.clear();
        List<FilePartition> offsetInfo = fileFetcher.snapshot();
        fileOffset.addAll(offsetInfo);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        ListStateDescriptor<FilePartition> descriptor = new ListStateDescriptor<>("file-offset", TypeInformation.of(FilePartition.class));
        fileOffset = stateStore.getListState(descriptor);
        if (context.isRestored()) {
            allocatedFiles = new ArrayList<>();
            for (FilePartition filePartition : fileOffset.get()) {
                allocatedFiles.add(filePartition);
            }
        }
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        fileFetcher = new FileFetcher<>(partitionId, sourceContext, deserializationSchema, allocatedFiles, sourceProperties);
        fileFetcher.open();
        fileFetcher.run();
    }

    @Override
    public void cancel() {
        if (fileFetcher != null) {
            fileFetcher.close();
        }
        if (aggScheduler != null) {
            aggScheduler.shutdownNow();
        }
        LOG.info("cancel file success");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (fileFetcher != null) {
            fileFetcher.close();
        }
        if (aggScheduler != null) {
            aggScheduler.shutdownNow();
        }
        LOG.info("close file success");
    }

    private void updateAgg() {
        if (fileFetcher == null) {
            return;
        }
        CleanInput cleanInput = new CleanInput(partitionId, partitionNum, fileFetcher.reachEnd());
        try {
            CleanOutput ret = aggregateManager.updateGlobalAggregate(CleanAggFunc.NAME, cleanInput, new CleanAggFunc());
            if (ret.isAllEnd()) {
                LOG.info("all data consumer end");
                Thread.sleep(300_000);
                cancelTotalJob();
            }
        } catch (Exception e) {
            LOG.info("update aggregateManager error", e);
        }
    }

    private void cancelTotalJob() throws Exception {
        JobMasterGateway jobMasterGateway = null;
        RpcInputSplitProvider splitProvider = (RpcInputSplitProvider) ((StreamingRuntimeContext) getRuntimeContext()).getInputSplitProvider();
        Field[] fields = RpcInputSplitProvider.class.getDeclaredFields();
        for (Field field : fields) {
            if (!StringUtils.equalsIgnoreCase(field.getName(), "jobMasterGateway")) {
                continue;
            }
            field.setAccessible(true);
            jobMasterGateway = (JobMasterGateway) field.get(splitProvider);
        }
        if (jobMasterGateway == null) {
            throw new RuntimeException("can't get jobMaster gateway address");
        }
        jobMasterGateway.cancel(Time.of(3, TimeUnit.SECONDS));

    }
}
