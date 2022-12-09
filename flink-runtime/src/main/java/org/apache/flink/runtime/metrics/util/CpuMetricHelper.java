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

package org.apache.flink.runtime.metrics.util;

import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;

/** CpuMetricHelper, get cpu usage. */
public class CpuMetricHelper {

    private final com.sun.management.OperatingSystemMXBean mxBean;

    private final int getLogicalProcessorCount;

    private volatile double usedCpuCores;

    private final int assignedCpuCores;

    public CpuMetricHelper(com.sun.management.OperatingSystemMXBean mxBean) {
        this.mxBean = mxBean;

        SystemInfo systemInfo = new SystemInfo();
        HardwareAbstractionLayer hardwareAbstractionLayer = systemInfo.getHardware();
        this.getLogicalProcessorCount =
                hardwareAbstractionLayer.getProcessor().getLogicalProcessorCount();
        this.assignedCpuCores = Runtime.getRuntime().availableProcessors();
    }

    public double getProcessCpuLoad() {
        usedCpuCores = mxBean.getProcessCpuLoad() * getLogicalProcessorCount;
        return usedCpuCores;
    }

    public double getProcessCpuUseagePercent() {
        return usedCpuCores / assignedCpuCores;
    }
}
