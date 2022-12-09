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

package org.apache.flink.runtime.metrics.sla;

import org.apache.flink.api.common.JobID;

/**
 * This strategy will only simply record the time of each start and end,
 * and the total cost needs to be calculated externally.
 */
public class NormalStrategy extends UnavailableMetricStoreStrategyBase {

    private NormalStrategy(JobID jobID) {
        super(jobID);
    }

    /**
     * The factory to instantiate {@link NormalStrategy}.
     */
    public static class Factory implements UnavailableMetricStoreStrategy.Factory {

        @Override
        public UnavailableMetricStoreStrategy create(JobID jobID) {
            return new NormalStrategy(jobID);
        }
    }


}
