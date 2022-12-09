/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.runtime.metrics.util;

import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;

/**
 * @author zhouxiaogang
 * @version $Id: CpuMetricHelper.java, v 0.1 2020-07-27 11:42
 * zhouxiaogang Exp $$
 */
public class CpuMetricHelper {

	private com.sun.management.OperatingSystemMXBean mxBean;

	private final int getLogicalProcessorCount;

	private volatile double usedCpuCores;

	private final int assignedCpuCores;

	public CpuMetricHelper(com.sun.management.OperatingSystemMXBean mxBean) {
		this.mxBean = mxBean;

		SystemInfo systemInfo = new SystemInfo();
		HardwareAbstractionLayer hardwareAbstractionLayer = systemInfo.getHardware();
		this.getLogicalProcessorCount = hardwareAbstractionLayer.getProcessor().getLogicalProcessorCount();
		this.assignedCpuCores = Runtime.getRuntime().availableProcessors();
	}

	public double getProcessCpuLoad() {
		usedCpuCores = mxBean.getProcessCpuLoad() * getLogicalProcessorCount;
		return usedCpuCores;
	}

	public double getProcessCpuUseagePercent(){
		return usedCpuCores / assignedCpuCores;
	}
}
