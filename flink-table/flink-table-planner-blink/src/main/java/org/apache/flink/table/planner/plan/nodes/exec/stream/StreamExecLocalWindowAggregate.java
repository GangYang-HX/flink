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
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGeneratorV2;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoListV2;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelectorV2;
import org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.LocalAggCombiner;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for window table-valued based local aggregate. */
public class StreamExecLocalWindowAggregate extends StreamExecWindowAggregateBase {

	private static final long WINDOW_AGG_MEMORY_RATIO = 100;

	private final int[] grouping;
	private final AggregateCall[] aggCalls;
	private final WindowingStrategy windowing;
	private final Transformation<RowData> inputTransform;

	public StreamExecLocalWindowAggregate(
		Transformation<RowData> inputTransform,
		int[] grouping,
		AggregateCall[] aggCalls,
		WindowingStrategy windowing,
		TypeInformation outputType,
		String description) {
		super(outputType, description);
		this.inputTransform = inputTransform;
		this.grouping = checkNotNull(grouping);
		this.aggCalls = checkNotNull(aggCalls);
		this.windowing = checkNotNull(windowing);
	}


	@SuppressWarnings("unchecked")
	@Override
	public Transformation<RowData> translateToPlanInternal(PlannerBase planner, TableConfig config) {
//		final ExecEdge inputEdge = getInputEdges().get(0);
//		final Transformation<RowData> inputTransform =
//			(Transformation<RowData>) inputEdge.translateToPlan(planner);
		final RowType inputRowType = ((RowDataTypeInfo) inputTransform.getOutputType()).toRowType();

		final ZoneId shiftTimeZone =
			TimeWindowUtil.getShiftTimeZone(
				windowing.getTimeAttributeType(),
				TableConfigUtils.getLocalTimeZone(config.getConfiguration()));
		final SliceAssigner sliceAssigner = createSliceAssigner(windowing, shiftTimeZone);

		final AggregateInfoListV2 aggInfoList =
			AggregateUtil.deriveStreamWindowAggregateInfoList(
				inputRowType,
				JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
				windowing.getWindow(),
				false); // isStateBackendDataViews

		final GeneratedNamespaceAggsHandleFunction<Long> generatedAggsHandler =
			createAggsHandler(
				sliceAssigner,
				aggInfoList,
				config,
				planner.getRelBuilder(),
				inputRowType.getChildren(),
				shiftTimeZone);
		final RowDataKeySelectorV2 selector =
			KeySelectorUtil.getRowDataSelectorV2(grouping, InternalTypeInfoV2.of(inputRowType));

		PagedTypeSerializer<RowData> keySer =
			(PagedTypeSerializer<RowData>) selector.getProducedType().toSerializer();
		AbstractRowDataSerializer<RowData> valueSer = new RowDataSerializer(inputRowType);

		WindowBuffer.LocalFactory bufferFactory =
			new RecordsWindowBuffer.LocalFactory(
				keySer, valueSer, new LocalAggCombiner.Factory(generatedAggsHandler));

		final OneInputStreamOperator<RowData, RowData> localAggOperator =
			new LocalSlicingWindowAggOperator(
				selector, sliceAssigner, bufferFactory, shiftTimeZone);

		return ExecNodeUtil.createOneInputTransformation(
			inputTransform,
			getDescription(),
			getFlinkRelTypeName(),
			SimpleOperatorFactory.of(localAggOperator),
			getOutputType(),
			inputTransform.getParallelism(),
			// use less memory here to let the chained head operator can have more memory
			WINDOW_AGG_MEMORY_RATIO / 2);
	}

	private GeneratedNamespaceAggsHandleFunction<Long> createAggsHandler(
		SliceAssigner sliceAssigner,
		AggregateInfoListV2 aggInfoList,
		TableConfig config,
		RelBuilder relBuilder,
		List<LogicalType> fieldTypes,
		ZoneId shiftTimeZone) {
		final AggsHandlerCodeGeneratorV2 generator =
			new AggsHandlerCodeGeneratorV2(
				new CodeGeneratorContext(config),
				relBuilder,
				JavaScalaConversionUtil.toScala(fieldTypes),
				true) // copyInputField
				.needAccumulate()
				.needMerge(0, true, null);

		return generator.generateNamespaceAggsHandler(
			"LocalWindowAggsHandler",
			aggInfoList,
			JavaScalaConversionUtil.toScala(Collections.emptyList()),
			sliceAssigner,
			shiftTimeZone);
	}
}
