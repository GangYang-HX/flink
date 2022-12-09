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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.WindowTableFunctionOperator;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.util.TimeWindowUtil;

import java.time.ZoneId;

import static org.apache.flink.table.planner.plan.utils.WindowTableFunctionUtil.createWindowAssigner;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} which acts as a table-valued function to assign a window for each row of
 * the input relation. The return value of the new relation includes all the original columns as
 * well additional 3 columns named {@code window_start}, {@code window_end}, {@code window_time} to
 * indicate the assigned window.
 */
public class StreamExecWindowTableFunction extends ExecNodeBase<RowData> {

    protected final TimeAttributeWindowingStrategy windowingStrategy;
    private Transformation<RowData> inputTransform;

    public StreamExecWindowTableFunction(
            Transformation<RowData> inputTransform,
            ReadableConfig readableConfig,
            TimeAttributeWindowingStrategy windowingStrategy,
            TypeInformation outputType,
            String description) {
        super(
                outputType,
                description);
        this.inputTransform = inputTransform;
        this.windowingStrategy = checkNotNull(windowingStrategy);
    }

    @Override
    public Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, TableConfig config) {
        WindowAssigner<TimeWindow> windowAssigner = createWindowAssigner(windowingStrategy);
        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowingStrategy.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config.getConfiguration()));
        WindowTableFunctionOperator windowTableFunctionOperator =
                new WindowTableFunctionOperator(
                        windowAssigner, windowingStrategy.getTimeAttributeIndex(), shiftTimeZone);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                getDescription(),
				getFlinkRelTypeName(),
                windowTableFunctionOperator,
                getOutputType(),
                inputTransform.getParallelism());
    }
}
