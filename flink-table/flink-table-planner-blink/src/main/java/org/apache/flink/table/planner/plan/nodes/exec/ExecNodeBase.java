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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;

import static org.apache.flink.table.planner.plan.utils.RelNodeTypeUtil.getNodeTypeName;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for {@link ExecNode}.
 *
 * @param <T> The type of the elements that result from this node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class ExecNodeBase<T> {

    private final String description;

    private final TypeInformation<RowData> outputType;

    protected ExecNodeBase(
            TypeInformation<RowData> outputType,
            String description) {
        this.outputType = checkNotNull(outputType);
        this.description = checkNotNull(description);
    }

    public String getDescription() {
        return description;
    }

    public TypeInformation<RowData> getOutputType() {
        return outputType;
    }

    /**
     * Internal method, translates this node into a Flink operator.
     *
     * @param planner The planner.
     * @param config per-{@link ExecNode} configuration that contains the merged configuration from
     *     various layers which all the nodes implementing this method should use, instead of
     *     retrieving configuration from the {@code planner}. For more details check {@link
     *     TableConfig}.
     */
    public abstract Transformation<T> translateToPlanInternal(
            PlannerBase planner, TableConfig config);

	public String getFlinkRelTypeName(){
		return getNodeTypeName(this);
	}
}
