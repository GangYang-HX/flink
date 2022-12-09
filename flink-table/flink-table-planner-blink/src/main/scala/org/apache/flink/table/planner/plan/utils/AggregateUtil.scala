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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{DataTypes, TableConfig, TableException}
import org.apache.flink.table.data.{DecimalData, RowData, StringData, TimestampData}
import org.apache.flink.table.dataview.MapViewTypeInfo
import org.apache.flink.table.expressions.ExpressionUtils.extractValue
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction, UserDefinedAggregateFunction, UserDefinedFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.dataview.DataViewUtils.{useNullSerializerForStateViewFieldsFromAccType, useNullSerializerForStateViewFieldsFromAccTypeV2}
import org.apache.flink.table.planner.dataview.{DataViewSpec, MapViewSpec}
import org.apache.flink.table.planner.expressions.{PlannerProctimeAttribute, PlannerRowtimeAttribute, PlannerWindowEnd, PlannerWindowStart}
import org.apache.flink.table.planner.functions.aggfunctions.{BuiltInAggregateFunction, DeclarativeAggregateFunction}
import org.apache.flink.table.planner.functions.sql.{FlinkSqlOperatorTable, SqlFirstLastValueAggFunction, SqlListAggFunction}
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.planner.plan.`trait`.{ModifyKindSetTraitDef, RelModifiedMonotonicity}
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.{fromDataTypeToLogicalType, fromLogicalTypeToDataType}
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot
import org.apache.flink.table.types.logical.{LogicalTypeRoot, _}
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.core.Aggregate.AggCallBinding
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.sql.`type`.SqlTypeUtil
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.{SqlAggFunction, SqlKind, SqlRankFunction}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction
import org.apache.flink.table.planner.functions.inference.{OperatorBindingCallContext, OperatorBindingCallContextV2}
import org.apache.flink.table.planner.plan.logical.{HoppingWindowSpec, WindowSpec}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel
import org.apache.flink.table.planner.plan.utils.AggregateUtil.{createAggregateInfoFromBridgingFunction, createAggregateInfoFromInternalFunction, createAggregateInfoFromLegacyFunction, createImperativeAggregateInfo}
import org.apache.flink.table.planner.typeutils.DataViewUtils
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.dataview.DataViewSpecV2
import org.apache.flink.table.types.inference.TypeInferenceUtil
import org.apache.flink.table.types.utils.DataTypeUtils

import java.time.Duration
import java.util
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AggregateUtil extends Enumeration {

  /**
    * Returns whether any of the aggregates are accurate DISTINCT.
    *
    * @return Whether any of the aggregates are accurate DISTINCT
    */
  def containsAccurateDistinctCall(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && !call.isApproximate)
  }

  /**
    * Returns whether any of the aggregates are approximate DISTINCT.
    *
    * @return Whether any of the aggregates are approximate DISTINCT
    */
  def containsApproximateDistinctCall(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && call.isApproximate)
  }

  /**
    * Returns indices of group functions.
    */
  def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex.filter { case (call, _) =>
      call.getAggregation.getKind match {
        case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID => true
        case _ => false
      }
    }.map { case (_, idx) => idx }
  }

  /**
    * Check whether AUXILIARY_GROUP aggCalls is in the front of the given agg's aggCallList,
    * and whether aggCallList contain AUXILIARY_GROUP when the given agg's groupSet is empty
    * or the indicator is true.
    * Returns AUXILIARY_GROUP aggCalls' args and other aggCalls.
    *
    * @param agg aggregate
    * @return returns AUXILIARY_GROUP aggCalls' args and other aggCalls
    */
  def checkAndSplitAggCalls(agg: Aggregate): (Array[Int], Seq[AggregateCall]) = {
    var nonAuxGroupCallsStartIdx = -1

    val aggCalls = agg.getAggCallList
    aggCalls.zipWithIndex.foreach {
      case (call, idx) =>
        if (call.getAggregation == FlinkSqlOperatorTable.AUXILIARY_GROUP) {
          require(call.getArgList.size == 1)
        }
        if (nonAuxGroupCallsStartIdx >= 0) {
          // the left aggCalls should not be AUXILIARY_GROUP
          require(call.getAggregation != FlinkSqlOperatorTable.AUXILIARY_GROUP,
            "AUXILIARY_GROUP should be in the front of aggCall list")
        }
        if (nonAuxGroupCallsStartIdx < 0 &&
          call.getAggregation != FlinkSqlOperatorTable.AUXILIARY_GROUP) {
          nonAuxGroupCallsStartIdx = idx
        }
    }

    if (nonAuxGroupCallsStartIdx < 0) {
      nonAuxGroupCallsStartIdx = aggCalls.length
    }

    val (auxGroupCalls, otherAggCalls) = aggCalls.splitAt(nonAuxGroupCallsStartIdx)
    if (agg.getGroupCount == 0) {
      require(auxGroupCalls.isEmpty,
        "AUXILIARY_GROUP aggCalls should be empty when groupSet is empty")
    }

    val auxGrouping = auxGroupCalls.map(_.getArgList.head.toInt).toArray
    require(auxGrouping.length + otherAggCalls.length == aggCalls.length)
    (auxGrouping, otherAggCalls)
  }

  def checkAndGetFullGroupSet(agg: Aggregate): Array[Int] = {
    val (auxGroupSet, _) = checkAndSplitAggCalls(agg)
    agg.getGroupSet.toArray ++ auxGroupSet
  }

  def getOutputIndexToAggCallIndexMap(
      aggregateCalls: Seq[AggregateCall],
      inputType: RelDataType,
      orderKeyIdx: Array[Int] = null): util.Map[Integer, Integer] = {
    val aggInfos = transformToAggregateInfoList(
      aggregateCalls,
      inputType,
      orderKeyIdx,
      Array.fill(aggregateCalls.size)(false),
      needInputCount = false,
      isStateBackedDataViews = false,
      needDistinctInfo = false).aggInfos

    val map = new util.HashMap[Integer, Integer]()
    var outputIndex = 0
    aggregateCalls.indices.foreach {
      aggCallIndex =>
        val aggInfo = aggInfos(aggCallIndex)
        val aggBuffers = aggInfo.externalAccTypes
        aggBuffers.indices.foreach { bufferIndex =>
          map.put(outputIndex + bufferIndex, aggCallIndex)
        }
        outputIndex += aggBuffers.length
    }
    map
  }

  def deriveAggregateInfoList(
      aggNode: StreamPhysicalRel,
      aggCalls: Seq[AggregateCall],
      grouping: Array[Int]): AggregateInfoList = {
    val input = aggNode.getInput(0)
    // need to call `retract()` if input contains update or delete
    val modifyKindSetTrait = input.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
    val needRetraction = if (modifyKindSetTrait == null) {
      // FlinkChangelogModeInferenceProgram is not applied yet, false as default
      false
    } else {
      !modifyKindSetTrait.modifyKindSet.isInsertOnly
    }
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(aggNode.getCluster.getMetadataQuery)
    val monotonicity = fmq.getRelModifiedMonotonicity(aggNode)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      grouping.length, needRetraction, monotonicity, aggCalls)
    AggregateUtil.transformToStreamAggregateInfoList(
      aggCalls,
      input.getRowType,
      needRetractionArray,
      needInputCount = needRetraction,
      isStateBackendDataViews = true)
  }

  def transformToBatchAggregateFunctions(
      aggregateCalls: Seq[AggregateCall],
      inputRowType: RelDataType,
      orderKeyIdx: Array[Int] = null)
  : (Array[Array[Int]], Array[Array[DataType]], Array[UserDefinedFunction]) = {

    val aggInfos = transformToAggregateInfoList(
      aggregateCalls,
      inputRowType,
      orderKeyIdx,
      Array.fill(aggregateCalls.size)(false),
      needInputCount = false,
      isStateBackedDataViews = false,
      needDistinctInfo = false).aggInfos

    val aggFields = aggInfos.map(_.argIndexes)
    val bufferTypes = aggInfos.map(_.externalAccTypes)
    val functions = aggInfos.map(_.function)

    (aggFields, bufferTypes, functions)
  }

  def transformToBatchAggregateInfoList(
      aggregateCalls: Seq[AggregateCall],
      inputRowType: RelDataType,
      orderKeyIdx: Array[Int] = null,
      needRetractions: Array[Boolean] = null): AggregateInfoList = {

    val needRetractionArray = if (needRetractions == null) {
      Array.fill(aggregateCalls.size)(false)
    } else {
      needRetractions
    }

    transformToAggregateInfoList(
      aggregateCalls,
      inputRowType,
      orderKeyIdx,
      needRetractionArray,
      needInputCount = false,
      isStateBackedDataViews = false,
      needDistinctInfo = false)
  }

  def transformToStreamAggregateInfoList(
      aggregateCalls: Seq[AggregateCall],
      inputRowType: RelDataType,
      needRetraction: Array[Boolean],
      needInputCount: Boolean,
      isStateBackendDataViews: Boolean,
      needDistinctInfo: Boolean = true): AggregateInfoList = {
    transformToAggregateInfoList(
      aggregateCalls,
      inputRowType,
      orderKeyIdx = null,
      needRetraction ++ Array(needInputCount), // for additional count(*)
      needInputCount,
      isStateBackendDataViews,
      needDistinctInfo)
  }

  def deriveStreamWindowAggregateInfoList(
    inputRowType: RowType,
    aggCalls: Seq[AggregateCall],
    windowSpec: WindowSpec,
    isStateBackendDataViews: Boolean): AggregateInfoListV2 = {
    // Hopping window requires additional COUNT(*) to determine  whether to register next timer
    // through whether the current fired window is empty, see SliceSharedWindowAggProcessor.
    val needInputCount = windowSpec.isInstanceOf[HoppingWindowSpec]
    val aggSize = if (needInputCount) {
      // we may insert a count(*) when need input count
      aggCalls.length + 1
    } else {
      aggCalls.length
    }
    // TODO: derive retraction flags from ChangelogMode trait when we support retraction for window
    val aggCallNeedRetractions = new Array[Boolean](aggSize)
    transformToAggregateInfoListV2(
      inputRowType,
      aggCalls,
      aggCallNeedRetractions,
      orderKeyIndexes = null,
      needInputCount,
      Option.empty[Int],
      isStateBackendDataViews,
      needDistinctInfo = true,
      isBounded = false)
  }

  /**
   * Transforms calcite aggregate calls to AggregateInfos.
   *
   * @param inputRowType     the input's output RowType
   * @param aggregateCalls   the calcite aggregate calls
   * @param aggCallNeedRetractions   whether the aggregate function need retract method
   * @param orderKeyIndexes      the index of order by field in the input, null if not over agg
   * @param needInputCount   whether need to calculate the input counts, which is used in
   *                         aggregation with retraction input.If needed,
   *                         insert a count(1) aggregate into the agg list.
   * @param indexOfExistingCountStar the index for the existing count star
   * @param isStateBackedDataViews   whether the dataview in accumulator use state or heap
   * @param needDistinctInfo  whether need to extract distinct information
   */
  private def transformToAggregateInfoListV2(
    inputRowType: RowType,
    aggregateCalls: Seq[AggregateCall],
    aggCallNeedRetractions: Array[Boolean],
    orderKeyIndexes: Array[Int],
    needInputCount: Boolean,
    indexOfExistingCountStar: Option[Int],
    isStateBackedDataViews: Boolean,
    needDistinctInfo: Boolean,
    isBounded: Boolean): AggregateInfoListV2 = {

    // Step-1:
    // if need inputCount, find count1 in the existed aggregate calls first,
    // if not exist, insert a new count1 and remember the index
    val (indexOfCountStar, countStarInserted, aggCalls) = insertCountStarAggCallV2(
      needInputCount,
      indexOfExistingCountStar,
      aggregateCalls)

    // Step-2:
    // extract distinct information from aggregate calls
    val (distinctInfos, newAggCalls) = extractDistinctInformationV2(
      needDistinctInfo,
      aggCalls,
      inputRowType,
      isStateBackedDataViews,
      needInputCount) // needInputCount means whether the aggregate consume retractions

    // Step-3:
    // create aggregate information
    val factory = new AggFunctionFactoryV2(
      inputRowType,
      orderKeyIndexes,
      aggCallNeedRetractions,
      isBounded)
    val aggInfos = newAggCalls
      .zipWithIndex
      .map { case (call, index) =>
        val argIndexes = call.getAggregation match {
          case _: SqlRankFunction => if (orderKeyIndexes != null) orderKeyIndexes else Array[Int]()
          case _ => call.getArgList.map(_.intValue()).toArray
        }
        transformToAggregateInfo(
          inputRowType,
          call,
          index,
          argIndexes,
          factory.createAggFunction(call, index),
          isStateBackedDataViews,
          aggCallNeedRetractions(index))
      }

    AggregateInfoListV2(aggInfos.toArray, indexOfCountStar, countStarInserted, distinctInfos)
  }

  private def transformToAggregateInfo(
    inputRowType: RowType,
    call: AggregateCall,
    index: Int,
    argIndexes: Array[Int],
    udf: UserDefinedFunction,
    hasStateBackedDataViews: Boolean,
    needsRetraction: Boolean)
  : AggregateInfoV2 = call.getAggregation match {

    case _: BridgingSqlAggFunction =>
      createAggregateInfoFromBridgingFunction(
        inputRowType,
        call,
        index,
        argIndexes,
        hasStateBackedDataViews,
        needsRetraction)

    case _: AggSqlFunction =>
      createAggregateInfoFromLegacyFunction(
        inputRowType,
        call,
        index,
        argIndexes,
        udf.asInstanceOf[UserDefinedAggregateFunction[_, _]],
        hasStateBackedDataViews,
        needsRetraction)

    case _: SqlAggFunction =>
      createAggregateInfoFromInternalFunction(
        call,
        udf,
        index,
        argIndexes,
        needsRetraction,
        hasStateBackedDataViews)
  }

  private def createAggregateInfoFromBridgingFunction(
    inputRowType: RowType,
    call: AggregateCall,
    index: Int,
    argIndexes: Array[Int],
    hasStateBackedDataViews: Boolean,
    needsRetraction: Boolean)
  : AggregateInfoV2 = {

    val function = call.getAggregation.asInstanceOf[BridgingSqlAggFunction]
    val definition = function.getDefinition
    val dataTypeFactory = function.getDataTypeFactory

    // not all information is available in the call context of aggregate functions at this location
    // e.g. literal information is lost because the aggregation is split into multiple operators
    val callContext = new OperatorBindingCallContextV2(
      dataTypeFactory,
      definition,
      new AggCallBinding(
        function.getTypeFactory,
        function,
        SqlTypeUtil.projectTypes(
          FlinkTypeFactory.INSTANCE.buildRelNodeRowTypeV2(inputRowType),
          argIndexes.map(Int.box).toList),
        0,
        false),
      call.getType)

    // create the final UDF for runtime
    val udf = UserDefinedFunctionHelper.createSpecializedFunction(
      function.getName,
      definition,
      callContext,
      classOf[PlannerBase].getClassLoader,
      null) // currently, aggregate functions have no access to configuration
    val inference = udf.getTypeInference(dataTypeFactory)

    // enrich argument types with conversion class
    val adaptedCallContext = TypeInferenceUtil.adaptArguments(
      inference,
      callContext,
      null)
    val enrichedArgumentDataTypes = toScala(adaptedCallContext.getArgumentDataTypes)

    // derive accumulator type with conversion class
    val enrichedAccumulatorDataType = TypeInferenceUtil.inferOutputType(
      adaptedCallContext,
      inference.getAccumulatorTypeStrategy.orElse(inference.getOutputTypeStrategy))

    // enrich output types with conversion class
    val enrichedOutputDataType = TypeInferenceUtil.inferOutputType(
      adaptedCallContext,
      inference.getOutputTypeStrategy)

    createImperativeAggregateInfo(
      call,
      udf.asInstanceOf[UserDefinedAggregateFunction[_, _]],
      index,
      argIndexes,
      enrichedArgumentDataTypes.toArray,
      enrichedAccumulatorDataType,
      enrichedOutputDataType,
      needsRetraction,
      hasStateBackedDataViews)
  }

  private def createImperativeAggregateInfo(
    call: AggregateCall,
    udf: UserDefinedAggregateFunction[_, _],
    index: Int,
    argIndexes: Array[Int],
    inputDataTypes: Array[DataType],
    accumulatorDataType: DataType,
    outputDataType: DataType,
    needsRetraction: Boolean,
    hasStateBackedDataViews: Boolean)
  : AggregateInfoV2 = {

    // extract data views and adapt the data views in the accumulator type
    // if a view is backed by a state backend
    val dataViewSpecs: Array[DataViewSpecV2] = if (hasStateBackedDataViews) {
      DataViewUtils.extractDataViews(index, accumulatorDataType).asScala.toArray
    } else {
      Array()
    }
    val adjustedAccumulatorDataType =
      DataViewUtils.adjustDataViews(accumulatorDataType, hasStateBackedDataViews)

    AggregateInfoV2(
      call,
      udf,
      index,
      argIndexes,
      inputDataTypes,
      Array(adjustedAccumulatorDataType),
      dataViewSpecs,
      outputDataType,
      needsRetraction)
  }

  private def createAggregateInfoFromLegacyFunction(
    inputRowType: RowType,
    call: AggregateCall,
    index: Int,
    argIndexes: Array[Int],
    udf: UserDefinedFunction,
    hasStateBackedDataViews: Boolean,
    needsRetraction: Boolean)
  : AggregateInfoV2 = {
    val (externalArgTypes, externalAccTypes, viewSpecs, externalResultType) = udf match {
      case a: UserDefinedAggregateFunction[_, _] =>
        val (implicitAccType, implicitResultType) = call.getAggregation match {
          case aggSqlFun: AggSqlFunction =>
            (aggSqlFun.externalAccType, aggSqlFun.externalResultType)
          case _ => (null, null)
        }
        val externalAccType = getAccumulatorTypeOfAggregateFunction(a, implicitAccType)
        val argTypes = call.getArgList
          .map(idx => inputRowType.getChildren.get(idx))
        val externalArgTypes: Array[DataType] = getAggUserDefinedInputTypes(
          a,
          externalAccType,
          argTypes.toArray)
        val (newExternalAccType, specs) = useNullSerializerForStateViewFieldsFromAccTypeV2(
          index,
          a,
          externalAccType,
          hasStateBackedDataViews)
        (
          externalArgTypes,
          Array(newExternalAccType),
          specs,
          getResultTypeOfAggregateFunction(a, implicitResultType)
        )

      case _ => throw new TableException(s"Unsupported function: $udf")
    }

    AggregateInfoV2(
      call,
      udf,
      index,
      argIndexes,
      externalArgTypes,
      externalAccTypes,
      viewSpecs,
      externalResultType,
      needsRetraction)
  }

  private def createAggregateInfoFromInternalFunction(
    call: AggregateCall,
    udf: UserDefinedFunction,
    index: Int,
    argIndexes: Array[Int],
    needsRetraction: Boolean,
    hasStateBackedDataViews: Boolean)
  : AggregateInfoV2 = udf match {

    case imperativeFunction: BuiltInAggregateFunction[_, _] =>
      createImperativeAggregateInfo(
        call,
        imperativeFunction,
        index,
        argIndexes,
        imperativeFunction.getArgumentDataTypes.asScala.toArray,
        imperativeFunction.getAccumulatorDataType,
        imperativeFunction.getOutputDataType,
        needsRetraction,
        hasStateBackedDataViews)

    case declarativeFunction: DeclarativeAggregateFunction =>
      AggregateInfoV2(
        call,
        udf,
        index,
        argIndexes,
        null,
        declarativeFunction.getAggBufferTypes,
        Array(),
        declarativeFunction.getResultType,
        needsRetraction)
  }


  /**
    * Transforms calcite aggregate calls to AggregateInfos.
    *
    * @param aggregateCalls   the calcite aggregate calls
    * @param inputRowType     the input rel data type
    * @param orderKeyIdx      the index of order by field in the input, null if not over agg
    * @param needRetraction   whether the aggregate function need retract method
    * @param needInputCount   whether need to calculate the input counts, which is used in
    *                         aggregation with retraction input.If needed,
    *                         insert a count(1) aggregate into the agg list.
    * @param isStateBackedDataViews   whether the dataview in accumulator use state or heap
    * @param needDistinctInfo  whether need to extract distinct information
    */
  private def transformToAggregateInfoList(
      aggregateCalls: Seq[AggregateCall],
      inputRowType: RelDataType,
      orderKeyIdx: Array[Int],
      needRetraction: Array[Boolean],
      needInputCount: Boolean,
      isStateBackedDataViews: Boolean,
      needDistinctInfo: Boolean): AggregateInfoList = {

    // Step-1:
    // if need inputCount, find count1 in the existed aggregate calls first,
    // if not exist, insert a new count1 and remember the index
    val (indexOfCountStar, countStarInserted, aggCalls) = insertCountStarAggCall(
      needInputCount,
      aggregateCalls)

    // Step-2:
    // extract distinct information from aggregate calls
    val (distinctInfos, newAggCalls) = extractDistinctInformation(
      needDistinctInfo,
      aggCalls,
      inputRowType,
      isStateBackedDataViews,
      needInputCount) // needInputCount means whether the aggregate consume retractions

    // Step-3:
    // create aggregate information
    val factory = new AggFunctionFactory(inputRowType, orderKeyIdx, needRetraction)
    val aggInfos = newAggCalls.zipWithIndex.map { case (call, index) =>
      val argIndexes = call.getAggregation match {
        case _: SqlRankFunction => orderKeyIdx
        case _ => call.getArgList.map(_.intValue()).toArray
      }

      val function = factory.createAggFunction(call, index)
      val (externalAccTypes, viewSpecs, externalResultType) = function match {
        case a: DeclarativeAggregateFunction =>
          val bufferTypes: Array[LogicalType] = a.getAggBufferTypes.map(_.getLogicalType)
          val bufferTypeInfos = bufferTypes.map(fromLogicalTypeToDataType)
          (bufferTypeInfos, Array.empty[DataViewSpec],
              fromLogicalTypeToDataType(a.getResultType.getLogicalType))
        case a: UserDefinedAggregateFunction[_, _] =>
          val (implicitAccType, implicitResultType) = call.getAggregation match {
            case aggSqlFun: AggSqlFunction =>
              (aggSqlFun.externalAccType, aggSqlFun.externalResultType)
            case _ => (null, null)
          }
          val externalAccType = getAccumulatorTypeOfAggregateFunction(a, implicitAccType)
          val (newExternalAccType, specs) = useNullSerializerForStateViewFieldsFromAccType(
            index,
            a,
            externalAccType,
            isStateBackedDataViews)
          (Array(newExternalAccType), specs,
            getResultTypeOfAggregateFunction(a, implicitResultType))
        case _ => throw new TableException(s"Unsupported function: $function")
      }

      AggregateInfo(
        call,
        function,
        index,
        argIndexes,
        externalAccTypes,
        viewSpecs,
        externalResultType,
        needRetraction(index))

    }.toArray

    AggregateInfoList(aggInfos, indexOfCountStar, countStarInserted, distinctInfos)
  }


  /**
    * Inserts an COUNT(*) aggregate call if needed. The COUNT(*) aggregate call is used
    * to count the number of added and retracted input records.
    * @param needInputCount whether to insert an InputCount aggregate
    * @param aggregateCalls original aggregate calls
    * @return (indexOfCountStar, countStarInserted, newAggCalls)
    */
  private def insertCountStarAggCall(
      needInputCount: Boolean,
      aggregateCalls: Seq[AggregateCall]): (Option[Int], Boolean, Seq[AggregateCall]) = {

    var indexOfCountStar: Option[Int] = None
    var countStarInserted: Boolean = false
    if (!needInputCount) {
      return (indexOfCountStar, countStarInserted, aggregateCalls)
    }

    // if need inputCount, find count(*) in the existed aggregate calls first,
    // if not exist, insert a new count(*) and remember the index
    var newAggCalls = aggregateCalls
    aggregateCalls.zipWithIndex.foreach { case (call, index) =>
      if (call.getAggregation.isInstanceOf[SqlCountAggFunction] &&
        call.filterArg < 0 &&
        call.getArgList.isEmpty &&
        !call.isApproximate &&
        !call.isDistinct) {
        indexOfCountStar = Some(index)
      }
    }

    // count(*) not exist in aggregateCalls, insert a count(*) in it.
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    if (indexOfCountStar.isEmpty) {

      val count1 = AggregateCall.create(
        SqlStdOperatorTable.COUNT,
        false,
        false,
        new util.ArrayList[Integer](),
        -1,
        typeFactory.createFieldTypeFromLogicalType(new BigIntType()),
        "_$count1$_")

      indexOfCountStar = Some(aggregateCalls.length)
      countStarInserted = true
      newAggCalls = aggregateCalls ++ Seq(count1)
    }

    (indexOfCountStar, countStarInserted, newAggCalls)
  }

  /**
   * Inserts an COUNT(*) aggregate call if needed. The COUNT(*) aggregate call is used
   * to count the number of added and retracted input records.
   *
   * @param needInputCount whether to insert an InputCount aggregate
   * @param aggregateCalls original aggregate calls
   * @param indexOfExistingCountStar the index for the existing count star
   * @return (indexOfCountStar, countStarInserted, newAggCalls)
   */
  private def insertCountStarAggCallV2(
    needInputCount: Boolean,
    indexOfExistingCountStar: Option[Int],
    aggregateCalls: Seq[AggregateCall]): (Option[Int], Boolean, Seq[AggregateCall]) = {

    if (indexOfExistingCountStar.getOrElse(-1) >= 0) {
      require(needInputCount)
      return (indexOfExistingCountStar, false, aggregateCalls)
    }

    var indexOfCountStar: Option[Int] = None
    var countStarInserted: Boolean = false
    if (!needInputCount) {
      return (indexOfCountStar, countStarInserted, aggregateCalls)
    }

    // if need inputCount, find count(*) in the existed aggregate calls first,
    // if not exist, insert a new count(*) and remember the index
    var newAggCalls = aggregateCalls
    aggregateCalls.zipWithIndex.foreach { case (call, index) =>
      if (call.getAggregation.isInstanceOf[SqlCountAggFunction] &&
        call.filterArg < 0 &&
        call.getArgList.isEmpty &&
        !call.isApproximate &&
        !call.isDistinct) {
        indexOfCountStar = Some(index)
      }
    }

    // count(*) not exist in aggregateCalls, insert a count(*) in it.
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    if (indexOfCountStar.isEmpty) {

      val count1 = AggregateCall.create(
        SqlStdOperatorTable.COUNT,
        false,
        false,
        new util.ArrayList[Integer](),
        -1,
        typeFactory.createFieldTypeFromLogicalType(new BigIntType()),
        "_$count1$_")

      indexOfCountStar = Some(aggregateCalls.length)
      countStarInserted = true
      newAggCalls = aggregateCalls ++ Seq(count1)
    }

    (indexOfCountStar, countStarInserted, newAggCalls)
  }

  /**
    * Extracts DistinctInfo array from the aggregate calls,
    * and change the distinct aggregate to non-distinct aggregate.
    *
    * @param needDistinctInfo whether to extract distinct information
    * @param aggCalls   the original aggregate calls
    * @param inputType  the input rel data type
    * @param isStateBackedDataViews whether the dataview in accumulator use state or heap
    * @param consumeRetraction  whether the distinct aggregate consumes retraction messages
    * @return (distinctInfoArray, newAggCalls)
    */
  private def extractDistinctInformation(
      needDistinctInfo: Boolean,
      aggCalls: Seq[AggregateCall],
      inputType: RelDataType,
      isStateBackedDataViews: Boolean,
      consumeRetraction: Boolean): (Array[DistinctInfo], Seq[AggregateCall]) = {

    if (!needDistinctInfo) {
      return (Array(), aggCalls)
    }

    val distinctMap = mutable.LinkedHashMap.empty[String, DistinctInfo]
    val newAggCalls = aggCalls.zipWithIndex.map { case (call, index) =>
      val argIndexes = call.getArgList.map(_.intValue()).toArray

      // extract distinct information and replace a new call
      if (call.isDistinct && !call.isApproximate && argIndexes.length > 0) {
        val argTypes: Array[LogicalType] = call
          .getArgList
          .map(inputType.getFieldList.get(_).getType)
          .map(FlinkTypeFactory.toLogicalType)
          .toArray

        val keyType = createDistinctKeyType(argTypes)
        val distinctInfo = distinctMap.getOrElseUpdate(
          argIndexes.mkString(","),
          DistinctInfo(
            argIndexes,
            keyType,
            null, // later fill in
            excludeAcc = false,
            null, // later fill in
            consumeRetraction,
            ArrayBuffer.empty[Int],
            ArrayBuffer.empty[Int]))
        // add current agg to the distinct agg list
        distinctInfo.filterArgs += call.filterArg
        distinctInfo.aggIndexes += index

        AggregateCall.create(
          call.getAggregation,
          false,
          false,
          call.getArgList,
          -1, // remove filterArg
          call.getType,
          call.getName)
      } else {
        call
      }
    }

    // fill in the acc type and dataview spec
    val distinctInfos = distinctMap.values.zipWithIndex.map { case (d, index) =>
      val valueType = if (consumeRetraction) {
        if (d.filterArgs.length <= 1) {
          Types.LONG
        } else {
          Types.PRIMITIVE_ARRAY(Types.LONG)
        }
      } else {
        if (d.filterArgs.length <= 64) {
          Types.LONG
        } else {
          Types.PRIMITIVE_ARRAY(Types.LONG)
        }
      }

      val accTypeInfo = new MapViewTypeInfo(
        // distinct is internal code gen, use internal type serializer.
        fromDataTypeToTypeInfo(d.keyType),
        valueType,
        isStateBackedDataViews,
        // the mapview serializer should handle null keys
        true)

      val accDataType = fromLegacyInfoToDataType(accTypeInfo)

      val distinctMapViewSpec = if (isStateBackedDataViews) {
        Some(MapViewSpec(
          s"distinctAcc_$index",
          -1, // the field index will not be used
          accTypeInfo))
      } else {
        None
      }

      DistinctInfo(
        d.argIndexes,
        d.keyType,
        accDataType,
        excludeAcc = false,
        distinctMapViewSpec,
        consumeRetraction,
        d.filterArgs,
        d.aggIndexes)
    }

    (distinctInfos.toArray, newAggCalls)
  }

  /**
   * Extracts DistinctInfo array from the aggregate calls,
   * and change the distinct aggregate to non-distinct aggregate.
   *
   * @param needDistinctInfo whether to extract distinct information
   * @param aggCalls   the original aggregate calls
   * @param inputType  the input rel data type
   * @param hasStateBackedDataViews whether the dataview in accumulator use state or heap
   * @param consumeRetraction  whether the distinct aggregate consumes retraction messages
   * @return (distinctInfoArray, newAggCalls)
   */
  private def extractDistinctInformationV2(
    needDistinctInfo: Boolean,
    aggCalls: Seq[AggregateCall],
    inputType: RowType,
    hasStateBackedDataViews: Boolean,
    consumeRetraction: Boolean): (Array[DistinctInfoV2], Seq[AggregateCall]) = {

    if (!needDistinctInfo) {
      return (Array(), aggCalls)
    }

    val distinctMap = mutable.LinkedHashMap.empty[String, DistinctInfo]
    val newAggCalls = aggCalls.zipWithIndex.map { case (call, index) =>
      val argIndexes = call.getArgList.map(_.intValue()).toArray

      // extract distinct information and replace a new call
      if (call.isDistinct && !call.isApproximate && argIndexes.length > 0) {
        val argTypes: Array[LogicalType] = call
          .getArgList
          .map(inputType.getChildren.get(_))
          .toArray

        val keyType = createDistinctKeyType(argTypes)
        val keyDataType = DataTypeUtils.toInternalDataType(keyType)
        val distinctInfo = distinctMap.getOrElseUpdate(
          argIndexes.mkString(","),
          DistinctInfo(
            argIndexes,
            keyDataType,
            null, // later fill in
            excludeAcc = false,
            null, // later fill in
            consumeRetraction,
            ArrayBuffer.empty[Int],
            ArrayBuffer.empty[Int]))
        // add current agg to the distinct agg list
        distinctInfo.filterArgs += call.filterArg
        distinctInfo.aggIndexes += index

        AggregateCall.create(
          call.getAggregation,
          false,
          false,
          call.getArgList,
          -1, // remove filterArg
          call.getType,
          call.getName)
      } else {
        call
      }
    }

    // fill in the acc type and data view spec
    val filterArgsLimit = if (consumeRetraction) {
      1
    } else {
      64
    }
    val distinctInfos = distinctMap.values.zipWithIndex.map { case (d, index) =>
      val distinctViewDataType = DataViewUtils.createDistinctViewDataType(
        d.keyType,
        d.filterArgs.length,
        filterArgsLimit)

      // create data views and adapt the data views in the accumulator type
      // if a view is backed by a state backend
      val distinctViewSpec = if (hasStateBackedDataViews) {
        Some(DataViewUtils.createDistinctViewSpec(index, distinctViewDataType))
      } else {
        None
      }
      val adjustedAccumulatorDataType =
        DataViewUtils.adjustDataViews(distinctViewDataType, hasStateBackedDataViews)

      DistinctInfoV2(
        d.argIndexes,
        d.keyType,
        adjustedAccumulatorDataType,
        excludeAcc = false,
        distinctViewSpec,
        consumeRetraction,
        d.filterArgs,
        d.aggIndexes)
    }

    (distinctInfos.toArray, newAggCalls)
  }

  def createDistinctKeyType(argTypes: Array[LogicalType]): DataType = {
    if (argTypes.length == 1) {
      argTypes(0).getTypeRoot match {
      case INTEGER => DataTypes.INT
      case BIGINT => DataTypes.BIGINT
      case SMALLINT => DataTypes.SMALLINT
      case TINYINT => DataTypes.TINYINT
      case FLOAT => DataTypes.FLOAT
      case DOUBLE => DataTypes.DOUBLE
      case BOOLEAN => DataTypes.BOOLEAN

      case DATE => DataTypes.INT
      case TIME_WITHOUT_TIME_ZONE => DataTypes.INT
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        val dt = argTypes(0).asInstanceOf[TimestampType]
        DataTypes.TIMESTAMP(dt.getPrecision).bridgedTo(classOf[TimestampData])
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val dt = argTypes(0).asInstanceOf[LocalZonedTimestampType]
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(dt.getPrecision).bridgedTo(classOf[TimestampData])

      case INTERVAL_YEAR_MONTH => DataTypes.INT
      case INTERVAL_DAY_TIME => DataTypes.BIGINT

      case VARCHAR =>
        val dt = argTypes(0).asInstanceOf[VarCharType]
        DataTypes.VARCHAR(dt.getLength).bridgedTo(classOf[StringData])
      case CHAR =>
        val dt = argTypes(0).asInstanceOf[CharType]
        DataTypes.CHAR(dt.getLength).bridgedTo(classOf[StringData])
      case DECIMAL =>
        val dt = argTypes(0).asInstanceOf[DecimalType]
        DataTypes.DECIMAL(dt.getPrecision, dt.getScale).bridgedTo(classOf[DecimalData])
      case t =>
        throw new TableException(s"Distinct aggregate function does not support type: $t.\n" +
          s"Please re-check the data type.")
      }
    } else {
      fromLogicalTypeToDataType(RowType.of(argTypes: _*)).bridgedTo(classOf[RowData])
    }
  }

  /**
    * Return true if all aggregates can be partially merged. False otherwise.
    */
  def doAllSupportPartialMerge(aggInfos: Array[AggregateInfo]): Boolean = {
    val supportMerge = aggInfos.map(_.function).forall {
      case _: DeclarativeAggregateFunction => true
      case a => ifMethodExistInFunction("merge", a)
    }

    //it means grouping without aggregate functions
    aggInfos.isEmpty || supportMerge
  }

  def doAllSupportPartialMergeV2(aggInfos: Array[AggregateInfoV2]): Boolean = {
    val supportMerge = aggInfos.map(_.function).forall {
      case _: DeclarativeAggregateFunction => true
      case a => ifMethodExistInFunction("merge", a)
    }

    //it means grouping without aggregate functions
    aggInfos.isEmpty || supportMerge
  }

  /**
    * Return true if all aggregates can be split. False otherwise.
    */
  def doAllAggSupportSplit(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.forall { aggCall =>
      aggCall.getAggregation match {
        case _: SqlCountAggFunction |
             _: SqlAvgAggFunction |
             _: SqlMinMaxAggFunction |
             _: SqlSumAggFunction |
             _: SqlSumEmptyIsZeroAggFunction |
             _: SqlSingleValueAggFunction |
             _: SqlListAggFunction => true
        case _: SqlFirstLastValueAggFunction => aggCall.getArgList.size() == 1
        case _ => false
      }
    }
  }

  /**
    * Derives output row type from stream local aggregate
    */
  def inferStreamLocalAggRowType(
      aggInfoList: AggregateInfoList,
      inputType: RelDataType,
      groupSet: Array[Int],
      typeFactory: FlinkTypeFactory): RelDataType = {
    val accTypes = aggInfoList.getAccTypes
    val groupingTypes = groupSet
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toLogicalType)
    val groupingNames = groupSet.map(inputType.getFieldNames.get(_))
    val accFieldNames = inferStreamAggAccumulatorNames(aggInfoList)

    typeFactory.buildRelNodeRowType(
      groupingNames ++ accFieldNames,
      groupingTypes ++ accTypes.map(fromDataTypeToLogicalType))
  }

  /**
    * Derives accumulators names from stream aggregate
    */
  def inferStreamAggAccumulatorNames(aggInfoList: AggregateInfoList): Array[String] = {
    var index = -1
    val aggBufferNames = aggInfoList.aggInfos.indices.flatMap { i =>
      aggInfoList.aggInfos(i).function match {
        case _: AggregateFunction[_, _] =>
          val name = aggInfoList.aggInfos(i).agg.getAggregation.getName.toLowerCase
          index += 1
          Array(s"$name$$$index")
        case daf: DeclarativeAggregateFunction =>
          daf.aggBufferAttributes.map { a =>
            index += 1
            s"${a.getName}$$$index"
          }
      }
    }
    val distinctBufferNames = aggInfoList.distinctInfos.indices.map { i =>
      s"distinct$$$i"
    }
    (aggBufferNames ++ distinctBufferNames).toArray
  }

  /**
    * Optimize max or min with retraction agg. MaxWithRetract can be optimized to Max if input is
    * update increasing.
    */
  def getNeedRetractions(
      groupCount: Int,
      needRetraction: Boolean,
      monotonicity: RelModifiedMonotonicity,
      aggCalls: Seq[AggregateCall]): Array[Boolean] = {
    val needRetractionArray = Array.fill(aggCalls.size)(needRetraction)
    if (monotonicity != null && needRetraction) {
      aggCalls.zipWithIndex.foreach { case (aggCall, idx) =>
        aggCall.getAggregation match {
          // if monotonicity is decreasing and aggCall is min with retract,
          // set needRetraction to false
          case a: SqlMinMaxAggFunction
            if a.getKind == SqlKind.MIN &&
              monotonicity.fieldMonotonicities(groupCount + idx) == SqlMonotonicity.DECREASING =>
            needRetractionArray(idx) = false
          // if monotonicity is increasing and aggCall is max with retract,
          // set needRetraction to false
          case a: SqlMinMaxAggFunction
            if a.getKind == SqlKind.MAX &&
              monotonicity.fieldMonotonicities(groupCount + idx) == SqlMonotonicity.INCREASING =>
            needRetractionArray(idx) = false
          case _ => // do nothing
        }
      }
    }

    needRetractionArray
  }

  /**
    * Derives output row type from local aggregate
    */
  def inferLocalAggRowType(
      aggInfoList: AggregateInfoList,
      inputRowType: RelDataType,
      groupSet: Array[Int],
      typeFactory: FlinkTypeFactory): RelDataType = {
    val accTypes = aggInfoList.getAccTypes
    val groupingTypes = groupSet
      .map(inputRowType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toLogicalType)
    val groupingNames = groupSet.map(inputRowType.getFieldNames.get(_))
    val accFieldNames = inferAggAccumulatorNames(aggInfoList)

    typeFactory.buildRelNodeRowType(
      groupingNames ++ accFieldNames,
      groupingTypes ++ accTypes.map(fromDataTypeToLogicalType))
  }

  /**
    * Derives accumulators names from aggregate
    */
  def inferAggAccumulatorNames(aggInfoList: AggregateInfoList): Array[String] = {
    var index = -1
    val aggBufferNames = aggInfoList.aggInfos.indices.flatMap { i =>
      aggInfoList.aggInfos(i).function match {
        case _: AggregateFunction[_, _] =>
          val name = aggInfoList.aggInfos(i).agg.getAggregation.getName.toLowerCase
          index += 1
          Array(s"$name$$$index")
        case daf: DeclarativeAggregateFunction =>
          daf.aggBufferAttributes.map { a =>
            index += 1
            s"${a.getName}$$$index"
          }
      }
    }
    val distinctBufferNames = aggInfoList.distinctInfos.indices.map { i =>
      s"distinct$$$i"
    }
    (aggBufferNames ++ distinctBufferNames).toArray
  }

  def inferAggAccumulatorNamesV2(aggInfoList: AggregateInfoListV2): Array[String] = {
    var index = -1
    val aggBufferNames = aggInfoList.aggInfos.indices.flatMap { i =>
      aggInfoList.aggInfos(i).function match {
        case _: AggregateFunction[_, _] =>
          val name = aggInfoList.aggInfos(i).agg.getAggregation.getName.toLowerCase
          index += 1
          Array(s"$name$$$index")
        case daf: DeclarativeAggregateFunction =>
          daf.aggBufferAttributes.map { a =>
            index += 1
            s"${a.getName}$$$index"
          }
      }
    }
    val distinctBufferNames = aggInfoList.distinctInfos.indices.map { i =>
      s"distinct$$$i"
    }
    (aggBufferNames ++ distinctBufferNames).toArray
  }

  /**
    * Creates a MiniBatch trigger depends on the config.
    */
  def createMiniBatchTrigger(tableConfig: TableConfig): CountBundleTrigger[RowData] = {
    val size = tableConfig.getConfiguration.getLong(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE)
    if (size <= 0 ) {
      throw new IllegalArgumentException(
        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE + " must be > 0.")
    }
    new CountBundleTrigger[RowData](size)
  }

  /**
    * Compute field index of given timeField expression.
    */
  def timeFieldIndex(
      inputType: RelDataType, relBuilder: RelBuilder, timeField: FieldReferenceExpression): Int = {
    relBuilder.values(inputType).field(timeField.getName).getIndex
  }

  /**
    * Computes the positions of (window start, window end, row time).
    */
  private[flink] def computeWindowPropertyPos(
      properties: Seq[PlannerNamedWindowProperty]): (Option[Int], Option[Int], Option[Int]) = {
    val propPos = properties.foldRight(
      (None: Option[Int], None: Option[Int], None: Option[Int], 0)) {
      case (p, (s, e, rt, i)) => p match {
        case PlannerNamedWindowProperty(_, prop) =>
          prop match {
            case PlannerWindowStart(_) if s.isDefined =>
              throw new TableException(
                "Duplicate window start property encountered. This is a bug.")
            case PlannerWindowStart(_) =>
              (Some(i), e, rt, i - 1)
            case PlannerWindowEnd(_) if e.isDefined =>
              throw new TableException("Duplicate window end property encountered. This is a bug.")
            case PlannerWindowEnd(_) =>
              (s, Some(i), rt, i - 1)
            case PlannerRowtimeAttribute(_) if rt.isDefined =>
              throw new TableException(
                "Duplicate window rowtime property encountered. This is a bug.")
            case PlannerRowtimeAttribute(_) =>
              (s, e, Some(i), i - 1)
            case PlannerProctimeAttribute(_) =>
              // ignore this property, it will be null at the position later
              (s, e, rt, i - 1)
          }
      }
    }
    (propPos._1, propPos._2, propPos._3)
  }

  def isRowtimeAttribute(field: FieldReferenceExpression): Boolean = {
    LogicalTypeChecks.isRowtimeAttribute(field.getOutputDataType.getLogicalType)
  }

  def isProctimeAttribute(field: FieldReferenceExpression): Boolean = {
    LogicalTypeChecks.isProctimeAttribute(field.getOutputDataType.getLogicalType)
  }

  def hasTimeIntervalType(intervalType: ValueLiteralExpression): Boolean = {
    hasRoot(intervalType.getOutputDataType.getLogicalType, LogicalTypeRoot.INTERVAL_DAY_TIME)
  }

  def hasRowIntervalType(intervalType: ValueLiteralExpression): Boolean = {
    hasRoot(intervalType.getOutputDataType.getLogicalType, LogicalTypeRoot.BIGINT)
  }

  def toLong(literalExpr: ValueLiteralExpression): JLong =
    extractValue(literalExpr, classOf[JLong]).get()

  def toDuration(literalExpr: ValueLiteralExpression): Duration =
    extractValue(literalExpr, classOf[Duration]).get()

  private[flink] def isTableAggregate(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls
      .filter(e => e.getAggregation.isInstanceOf[AggSqlFunction])
      .map(e => e.getAggregation.asInstanceOf[AggSqlFunction].aggregateFunction)
      .exists(_.isInstanceOf[TableAggregateFunction[_, _]])
  }
}
