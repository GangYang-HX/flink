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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;
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
import org.apache.flink.table.planner.plan.logical.*;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
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
import org.apache.flink.table.runtime.operators.window.slicing.SliceSharedAssigner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfoV2;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNodeBase} for window table-valued based aggregate.
 *
 * <p>The differences between {@link StreamExecWindowAggregate} and {@link
 * StreamExecWindowAggregate} is that, this node is translated from window TVF syntax, but the
 * other is from the legacy GROUP WINDOW FUNCTION syntax. In the long future, {@link
 * StreamExecWindowAggregate} will be dropped.
 */
public class StreamExecWindowAggregate extends StreamExecWindowAggregateBase {

    private static final long WINDOW_AGG_MEMORY_RATIO = 100;

    private final int[] grouping;
    private final AggregateCall[] aggCalls;
    private final WindowingStrategy windowing;
    private final NamedWindowProperty[] namedWindowProperties;
    private final Transformation<RowData> inputTransform;

    public StreamExecWindowAggregate(
            Transformation<RowData> inputTransform,
            int[] grouping,
            AggregateCall[] aggCalls,
            WindowingStrategy windowing,
            NamedWindowProperty[] namedWindowProperties,
            TypeInformation outputType,
            String description) {
        super( outputType, description);
        this.inputTransform = inputTransform;
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.windowing = checkNotNull(windowing);
        this.namedWindowProperties = checkNotNull(namedWindowProperties);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Transformation<RowData> translateToPlanInternal(PlannerBase planner, TableConfig config) {
        final RowType inputRowType = ((RowDataTypeInfo)inputTransform.getOutputType()).toRowType();
        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowing.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config.getConfiguration()));

        final SliceAssigner sliceAssigner = createSliceAssigner(windowing, shiftTimeZone);

        // Hopping window requires additional COUNT(*) to determine whether to register next timer
        // through whether the current fired window is empty, see SliceSharedWindowAggProcessor.
        final boolean[] aggCallNeedRetractions = new boolean[aggCalls.length];
        Arrays.fill(aggCallNeedRetractions, false);
        final AggregateInfoListV2 aggInfoList =
                AggregateUtil.deriveStreamWindowAggregateInfoList(
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        windowing.getWindow(),
                        true); // isStateBackendDataViews

        final GeneratedNamespaceAggsHandleFunction<Long> generatedAggsHandler =
                createAggsHandler(
                        sliceAssigner,
                        aggInfoList,
                        config,
                        planner.getRelBuilder(),
                        inputRowType.getChildren(),
                        shiftTimeZone);

        final LogicalType[] keyTypes =
                Arrays.stream(grouping)
                        .mapToObj(inputRowType::getTypeAt)
                        .toArray(LogicalType[]::new);

        final RowDataKeySelectorV2 selector =
                KeySelectorUtil.getRowDataSelectorV2(grouping, InternalTypeInfoV2.of(inputRowType));
        final LogicalType[] accTypes = convertToLogicalTypes(aggInfoList.getAccTypes());

        final OneInputStreamOperator<RowData, RowData> windowOperator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(new RowDataSerializer(inputRowType))
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer((PagedTypeSerializer<RowData>) selector.getProducedType().toSerializer())
                        .assigner(sliceAssigner)
                        .countStarIndex(aggInfoList.getIndexOfCountStar())
                        .aggregate(generatedAggsHandler, new RowDataSerializer(accTypes))
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
                        false) // copyInputField
                        .needAccumulate();

        if (sliceAssigner instanceof SliceSharedAssigner) {
            generator.needMerge(0, false, null);
        }

        final List<WindowProperty> windowProperties =
                Arrays.asList(
                        Arrays.stream(namedWindowProperties)
                                .map(NamedWindowProperty::getProperty)
                                .toArray(WindowProperty[]::new));

        return generator.generateNamespaceAggsHandler(
                "WindowAggsHandler",
                aggInfoList,
                JavaScalaConversionUtil.toScala(windowProperties),
                sliceAssigner,
                shiftTimeZone);
    }

}
