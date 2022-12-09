package org.apache.flink.runtime.metrics.util;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CGroupMetricHelper {

    private static final Logger LOG = LoggerFactory.getLogger(CGroupMetricHelper.class);

    public static final String CONTAINER_ID_TAG = "{container_id}";

    private static final String THROTTLED_TIME_METRIC_NAME = "throttled_time";

    private static final String NR_THROTTLED_METRIC_NAME = "nr_throttled";

    private static final String NR_PERIODS_METRIC_NAME = "nr_periods";

    private long detectIntervel;

    // metrics fields
    private volatile long cgroupCpuThrottle;

    private volatile long cgroupCpuNrThrottled;

    private volatile long cgroupCpuNrPeriods;

    public CGroupMetricHelper(ResourceID resourceID, String cGroupFilePath, long detectIntervel) {
        if (StringUtils.isNotEmpty(cGroupFilePath) && cGroupFilePath.contains(CONTAINER_ID_TAG)) {
            cGroupFilePath = cGroupFilePath.replace(CONTAINER_ID_TAG, resourceID.getResourceIdString());
            this.detectIntervel = detectIntervel;
            doDetect(cGroupFilePath);
        } else {
            LOG.error("There is a illeagal path: {}", cGroupFilePath);
        }
    }

    private void doDetect(String file) {
        Runnable task = () -> {
            try (BufferedReader in = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] metricNameAndValue = line.split(" ");
                    String metricName = metricNameAndValue[0];
                    long metricValue = Long.parseLong(metricNameAndValue[1]);
                    switch (metricName) {
                        case NR_PERIODS_METRIC_NAME:
                            cgroupCpuNrPeriods = metricValue;
                            break;
                        case NR_THROTTLED_METRIC_NAME:
                            cgroupCpuNrThrottled = metricValue;
                            break;
                        case THROTTLED_TIME_METRIC_NAME:
                            cgroupCpuThrottle = metricValue;
                            break;
                        default:
                            throw new IllegalArgumentException("unknow metric named: " + metricName);
                    }
                }
            } catch (FileNotFoundException ignored) {
                // maybe no throttle metrics. Ignore it!
            } catch (Exception e) {
                LOG.error("Read cgroup file content error. file : {}", file, e);
            }
        };
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
                new DispatcherThreadFactory(
                        Thread.currentThread().getThreadGroup(), "TM cgreoup metrics timer"));
        new ScheduledExecutorServiceAdapter(executorService).scheduleAtFixedRate(task, 1, detectIntervel, TimeUnit.SECONDS);
    }

    public long getCgroupCpuThrottle() {
        return cgroupCpuThrottle;
    }

    public long getCgroupCpuNrThrottled() {
        return cgroupCpuNrThrottled;
    }

    public long getCgroupCpuNrPeriods() {
        return cgroupCpuNrPeriods;
    }

}
