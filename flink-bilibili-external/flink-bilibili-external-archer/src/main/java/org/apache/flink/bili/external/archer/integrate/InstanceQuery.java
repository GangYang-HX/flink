package org.apache.flink.bili.external.archer.integrate;

import lombok.Data;

/**
 * query instance
 */
@Data
public class InstanceQuery {
    private Long jobId;
    private String jobName;

    /** 实例ID */
    private String instanceId;

    private Integer status;
    private Integer jobType;
    private Long projectId;
    private String projectName;
    private String owner;
    private String ownerId;
    private String departmentName;
    private String departmentId;
    private Integer periodType;
    private String bizTime;
    private String runStartTime;
    private String runEndTime;
    private Long costTime;
    private String queueName;
    private Integer priority;
    private String cron;
    private String ctime;
    private String mtime;
    private Integer executeType;
    private String batchName;
    private Long batchId;

    /** 调度周期类别 HOUR(0), DAY(1), WEEK(2), MONTH(3); */
    private Integer schedulePeriod;
}
