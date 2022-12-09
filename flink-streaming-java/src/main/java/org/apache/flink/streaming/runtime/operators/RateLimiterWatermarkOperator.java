package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.functions.source.ControlSpeed;
import org.apache.flink.streaming.api.functions.source.ControlSpeedAggFunction;
import org.apache.flink.streaming.api.functions.source.SubTaskStatus;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimiterWatermarkOperator<T> extends TimestampsAndWatermarksOperator<T> {

	protected static final Logger LOG = LoggerFactory.getLogger(RateLimiterWatermarkOperator.class);

	private static final long serialVersionUID = 1L;

	private StreamingRuntimeContext streamingRuntimeContext;

	private static final String aggName = "ratelimiter";

	/**
	 * The index of the parallel subtask.
	 */
	private volatile int taskId;

	/**
	 * Used to record how many counts it accepts.
	 */
	private final AtomicLong offset = new AtomicLong();

	/**
	 * Last schedule offset.
	 */
	private volatile long lastScheduleOffset;

	/**
	 * Number of records the task emit per second.
	 */
	private volatile long currSpeed;

	private volatile long lastScheduleWatermark;

	/**
	 * Tell the task how to limit speed.
	 */
	private volatile ControlSpeed controlSpeed;

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public RateLimiterWatermarkOperator(WatermarkStrategy<T> watermarkStrategy) {
		super(watermarkStrategy);
	}

	@Override
	public void open() throws Exception {
		super.open();

		Preconditions.checkArgument(getExecutionConfig().getSchedulerTime() == getExecutionConfig().getEstimateTime(),
			"The scheduling time and the estimated time must be equal.");

		LOG.info("rate limiter: " + getExecutionConfig().isRateLimiter());
		LOG.info("debugControlSpeed: " + getExecutionConfig().isDebugControlSpeed());
		LOG.info("tolerateInterval: " + getExecutionConfig().getTolerateInterval());
		LOG.info("schedulerTime: " + getExecutionConfig().getSchedulerTime());
		LOG.info("estimateTime: " + getExecutionConfig().getEstimateTime());

		streamingRuntimeContext = getRuntimeContext();
		controlSpeed = new ControlSpeed(Integer.MIN_VALUE, 0);
		ControlSpeedAggFunction controlSpeedAggFunction = new ControlSpeedAggFunction();

		controlSpeedAggFunction.setNumberOfParallelSubtasks(streamingRuntimeContext.getNumberOfParallelSubtasks());
		controlSpeedAggFunction.setTolerateInterval(getExecutionConfig().getTolerateInterval());
		controlSpeedAggFunction.setEstimateTime(getExecutionConfig().getEstimateTime());
		controlSpeedAggFunction.setDebugControlSpeed(getExecutionConfig().isDebugControlSpeed());
		GlobalAggregateManager globalAggregateManager = streamingRuntimeContext.getGlobalAggregateManager();

		ScheduledThreadPoolExecutor taskStatusExecutor = new ScheduledThreadPoolExecutor(1);
		taskStatusExecutor.scheduleAtFixedRate(() -> {
			Thread.currentThread().setName("UpdateTaskStatus-" + streamingRuntimeContext.getTaskNameWithSubtasks());
			SubTaskStatus subTaskStatus = new SubTaskStatus(taskId, getCurrentWatermark(), System.currentTimeMillis(), SubTaskStatus.TaskStatusType.UPDATE_TASK_STATUS);
			try {
				globalAggregateManager.updateGlobalAggregate(
					aggName,
					InstantiationUtil.serializeObject(subTaskStatus),
					controlSpeedAggFunction);
			} catch (Exception e) {
				LOG.warn("Failed to update task status.", e);
			}
		}, 50, 50, TimeUnit.MILLISECONDS);


		ScheduledThreadPoolExecutor rateLimiterExecutor = new ScheduledThreadPoolExecutor(1);
		rateLimiterExecutor.scheduleAtFixedRate(() -> {
			Thread.currentThread().setName("ControlSpeed-" + streamingRuntimeContext.getTaskNameWithSubtasks());
			long currOffset = offset.get();
			currSpeed = currOffset - lastScheduleOffset;
			lastScheduleWatermark = getCurrentWatermark();
			SubTaskStatus subTaskStatus = new SubTaskStatus(taskId, lastScheduleWatermark, System.currentTimeMillis(), SubTaskStatus.TaskStatusType.CONTROL_SPEED);
			lastScheduleWatermark = subTaskStatus.getWatermark();
			String taskName = "SourceTask(" + streamingRuntimeContext.getTaskNameWithSubtasks().substring(streamingRuntimeContext.getTaskNameWithSubtasks().length() - 5);
			try {
				LOG.info(taskName + ", offset: " + currOffset + ", lastScheduleOffset: " + lastScheduleOffset + ", speed: (offset-lastSchedulerOffset) / 3: " + currSpeed / 3d);
				lastScheduleOffset = currOffset;
				byte[] values = globalAggregateManager.updateGlobalAggregate(
					aggName,
					InstantiationUtil.serializeObject(subTaskStatus),
					controlSpeedAggFunction);
				controlSpeed = InstantiationUtil.deserializeObject(values, this.getClass().getClassLoader());
				LOG.info(
					taskName +
						", offset: " + currOffset +
						", wm: " + sdf.parse(sdf.format(subTaskStatus.getWatermark())) +
						", also: " + subTaskStatus.getWatermark() +
						", controlSpeed: " + controlSpeed.toString());
			} catch (Exception e) {
				LOG.warn("Failed to get control speed from jobmanager.", e);
			}
			// let the task run estimateTime first, then start calculate the task speed.
		}, getExecutionConfig().getEstimateTime(), getExecutionConfig().getSchedulerTime(), TimeUnit.MILLISECONDS);

	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		super.processElement(element);

		taskId = streamingRuntimeContext.getIndexOfThisSubtask();
		offset.incrementAndGet();

		// start rate limiter for the source task.
		if (controlSpeed.getEstimateTime() != Integer.MIN_VALUE) {

			String taskName = "SourceTask(" + streamingRuntimeContext.getTaskNameWithSubtasks().substring(streamingRuntimeContext.getTaskNameWithSubtasks().length() - 5);
			long targetWatermark = controlSpeed.getTargetWatermark();
			long gapWatermark = targetWatermark - lastScheduleWatermark;
			if (gapWatermark <= 0L) {
				LOG.info(taskName + ", gapWatermark: " + gapWatermark);
				try {
					LOG.info(taskName + ", max watermark(" + sdf.parse(sdf.format(lastScheduleWatermark)) + ")"
						+ " >= targetWm(" + sdf.parse(sdf.format(targetWatermark)) + ") task sleep for " + controlSpeed.getEstimateTime()
						+ "milliseconds." + ", ctime: " + sdf.parse(sdf.format(System.currentTimeMillis())));
					TimeUnit.MILLISECONDS.sleep(controlSpeed.getEstimateTime());
				} catch (Exception e) {
					LOG.warn(taskName + "Failed to parse date or sleep has something wrong.", e);
				}
			} else {
				if (getCurrentWatermark() >= targetWatermark) {
					try {
						LOG.info(taskName + ", currWm(" + sdf.parse(sdf.format(getCurrentWatermark())) + ") > "
							+ "targetWm(" + sdf.parse(sdf.format(targetWatermark)) + "), task will sleep for 100ms");
						TimeUnit.MILLISECONDS.sleep(100);
					} catch (Exception e) {
						LOG.warn(taskName + ", Failed to parse date or sleep has something wrong.", e);
					}
				}
			}
		}

	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		super.onProcessingTime(timestamp);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}
