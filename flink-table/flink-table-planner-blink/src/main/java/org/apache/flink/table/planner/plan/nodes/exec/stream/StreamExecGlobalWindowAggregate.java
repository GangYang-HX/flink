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
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
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
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelectorV2;
import org.apache.flink.table.runtime.operators.aggregate.window.SlicingWindowAggOperatorBuilder;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for window table-valued based global aggregate. */
public class StreamExecGlobalWindowAggregate extends StreamExecWindowAggregateBase {

	private static final long WINDOW_AGG_MEMORY_RATIO = 100;

	private final int[] grouping;
	private final AggregateCall[] aggCalls;
	private final WindowingStrategy windowing;
	private final NamedWindowProperty[] namedWindowProperties;
	private final Transformation<RowData> inputTransform;

	/** The input row type of this node's local agg. */
	private final RowType localAggInputRowType;

	public StreamExecGlobalWindowAggregate(
		Transformation<RowData> inputTransform,
		int[] grouping,
		AggregateCall[] aggCalls,
		WindowingStrategy windowing,
		NamedWindowProperty[] namedWindowProperties,
		RowType localAggInputRowType,
		TypeInformation outputType,
		String description) {
		super(outputType,description);
		this.inputTransform = inputTransform;
		this.grouping = checkNotNull(grouping);
		this.aggCalls = checkNotNull(aggCalls);
		this.windowing = checkNotNull(windowing);
		this.namedWindowProperties = checkNotNull(namedWindowProperties);
		this.localAggInputRowType = localAggInputRowType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Transformation<RowData> translateToPlanInternal(PlannerBase planner, TableConfig config) {
		final RowType inputRowType = ((RowDataTypeInfo) inputTransform.getOutputType()).toRowType();

		final ZoneId shiftTimeZone =
			TimeWindowUtil.getShiftTimeZone(
				windowing.getTimeAttributeType(),
				TableConfigUtils.getLocalTimeZone(config.getConfiguration()));
		final SliceAssigner sliceAssigner = createSliceAssigner(windowing, shiftTimeZone);

		final AggregateInfoListV2 localAggInfoList =
			AggregateUtil.deriveStreamWindowAggregateInfoList(
				localAggInputRowType, // should use original input here
				JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
				windowing.getWindow(),
				false); // isStateBackendDataViews

		final AggregateInfoListV2 globalAggInfoList =
			AggregateUtil.deriveStreamWindowAggregateInfoList(
				localAggInputRowType, // should use original input here
				JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
				windowing.getWindow(),
				true); // isStateBackendDataViews

		// handler used to merge multiple local accumulators into one accumulator,
		// where the accumulators are all on memory
		final GeneratedNamespaceAggsHandleFunction<Long> localAggsHandler =
			createAggsHandler(
				"LocalWindowAggsHandler",
				sliceAssigner,
				localAggInfoList,
				grouping.length,
				true,
				localAggInfoList.getAccTypes(),
				config,
				planner.getRelBuilder(),
				shiftTimeZone);

		// handler used to merge the single local accumulator (on memory) into state accumulator
		final GeneratedNamespaceAggsHandleFunction<Long> globalAggsHandler =
			createAggsHandler(
				"GlobalWindowAggsHandler",
				sliceAssigner,
				globalAggInfoList,
				0,
				true,
				localAggInfoList.getAccTypes(),
				config,
				planner.getRelBuilder(),
				shiftTimeZone);

		// handler used to merge state accumulators for merging slices into window,
		// e.g. Hop and Cumulate
		final GeneratedNamespaceAggsHandleFunction<Long> stateAggsHandler =
			createAggsHandler(
				"StateWindowAggsHandler",
				sliceAssigner,
				globalAggInfoList,
				0,
				false,
				globalAggInfoList.getAccTypes(),
				config,
				planner.getRelBuilder(),
				shiftTimeZone);

		final RowDataKeySelectorV2 selector =
			KeySelectorUtil.getRowDataSelectorV2(grouping, InternalTypeInfoV2.of(inputRowType));
		final LogicalType[] accTypes = convertToLogicalTypes(globalAggInfoList.getAccTypes());

		final OneInputStreamOperator<RowData, RowData> windowOperator =
			SlicingWindowAggOperatorBuilder.builder()
				.inputSerializer(new RowDataSerializer(inputRowType))
				.shiftTimeZone(shiftTimeZone)
				.keySerializer(
					(PagedTypeSerializer<RowData>)
						selector.getProducedType().toSerializer())
				.assigner(sliceAssigner)
				.countStarIndex(globalAggInfoList.getIndexOfCountStar())
				.globalAggregate(
					localAggsHandler,
					globalAggsHandler,
					stateAggsHandler,
					new RowDataSerializer(accTypes))
				.build();

		final OneInputTransformation<RowData, RowData> transform =
			ExecNodeUtil.createOneInputTransformation(
				inputTransform,
				getDescription(),
				getFlinkRelTypeName(),
				SimpleOperatorFactory.of(windowOperator),
				getOutputType(),
				inputTransform.getParallelism(),
				WINDOW_AGG_MEMORY_RATIO);

		// set KeyType and Selector for state
		transform.setStateKeySelector(selector);
		transform.setStateKeyType(selector.getProducedType());
		return transform;
	}

	private GeneratedNamespaceAggsHandleFunction<Long> createAggsHandler(
		String name,
		SliceAssigner sliceAssigner,
		AggregateInfoListV2 aggInfoList,
		int mergedAccOffset,
		boolean mergedAccIsOnHeap,
		DataType[] mergedAccExternalTypes,
		TableConfig config,
		RelBuilder relBuilder,
		ZoneId shifTimeZone) {
		final AggsHandlerCodeGeneratorV2 generator =
			new AggsHandlerCodeGeneratorV2(
				new CodeGeneratorContext(config),
				relBuilder,
				JavaScalaConversionUtil.toScala(localAggInputRowType.getChildren()),
				true) // copyInputField
				.needAccumulate()
				.needMerge(mergedAccOffset, mergedAccIsOnHeap, mergedAccExternalTypes);

		final List<WindowProperty> windowProperties =
			Arrays.asList(
				Arrays.stream(namedWindowProperties)
					.map(NamedWindowProperty::getProperty)
					.toArray(WindowProperty[]::new));

		return generator.generateNamespaceAggsHandler(
			name,
			aggInfoList,
			JavaScalaConversionUtil.toScala(windowProperties),
			sliceAssigner,
			shifTimeZone);
	}
}
