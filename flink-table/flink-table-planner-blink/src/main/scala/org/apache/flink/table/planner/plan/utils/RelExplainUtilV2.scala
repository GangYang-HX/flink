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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty

import org.apache.calcite.rel.`type`.RelDataType

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Explain rel utility methods.
 */
object RelExplainUtilV2 {
  def streamWindowAggregationToString(
    inputRowType: RelDataType,
    outputRowType: RelDataType,
    aggInfoList: AggregateInfoListV2,
    grouping: Array[Int],
    windowProperties: Seq[NamedWindowProperty],
    isLocal: Boolean = false,
    isGlobal: Boolean = false): String = {
    stringifyStreamAggregationToString(
      inputRowType,
      outputRowType,
      aggInfoList,
      grouping,
      shuffleKey = None,
      windowProperties,
      isLocal,
      isGlobal)
  }

  private def stringifyStreamAggregationToString(
    inputRowType: RelDataType,
    outputRowType: RelDataType,
    aggInfoList: AggregateInfoListV2,
    grouping: Array[Int],
    shuffleKey: Option[Array[Int]],
    windowProperties: Seq[NamedWindowProperty],
    isLocal: Boolean,
    isGlobal: Boolean): String = {

    val aggInfos = aggInfoList.aggInfos
    val actualAggInfos = aggInfoList.getActualAggregateInfos
    val distinctInfos = aggInfoList.distinctInfos
    val distinctFieldNames = distinctInfos.indices.map(index => s"distinct$$$index")
    // aggIndex -> distinctFieldName
    val distinctAggs = distinctInfos.zip(distinctFieldNames)
      .flatMap(f => f._1.aggIndexes.map(i => (i, f._2)))
      .toMap
    val aggFilters = {
      val distinctAggFilters = distinctInfos
        .flatMap(d => d.aggIndexes.zip(d.filterArgs))
        .toMap
      val otherAggFilters = aggInfos
        .map(info => (info.aggIndex, info.agg.filterArg))
        .toMap
      otherAggFilters ++ distinctAggFilters
    }

    val inFieldNames = inputRowType.getFieldNames.toList.toArray
    val outFieldNames = outputRowType.getFieldNames.toList.toArray
    val groupingNames = grouping.map(inFieldNames(_))
    val aggOffset = shuffleKey match {
      case None => grouping.length
      case Some(k) => k.length
    }
    val isIncremental: Boolean = shuffleKey.isDefined

    // TODO output local/global agg call names like Partial_XXX, Final_XXX
    val aggStrings = if (isLocal) {
      stringifyLocalAggregates(aggInfos, distinctInfos, distinctAggs, aggFilters, inFieldNames)
    } else if (isGlobal || isIncremental) {
      val accFieldNames = inputRowType.getFieldNames.toList.toArray
      val aggOutputFieldNames = localAggOutputFieldNames(aggOffset, aggInfos, accFieldNames)
      stringifyGlobalAggregates(aggInfos, distinctAggs, aggOutputFieldNames)
    } else {
      stringifyAggregates(actualAggInfos, distinctAggs, aggFilters, inFieldNames)
    }

    val isTableAggregate =
      AggregateUtil.isTableAggregate(aggInfoList.getActualAggregateCalls.toList)
    val outputFieldNames = if (isLocal) {
      grouping.map(inFieldNames(_)) ++ localAggOutputFieldNames(aggOffset, aggInfos, outFieldNames)
    } else if (isIncremental) {
      val accFieldNames = inputRowType.getFieldNames.toList.toArray
      grouping.map(inFieldNames(_)) ++ localAggOutputFieldNames(aggOffset, aggInfos, accFieldNames)
    } else if (isTableAggregate) {
      val groupingOutNames = outFieldNames.slice(0, grouping.length)
      val aggOutNames = List(s"(${outFieldNames.drop(grouping.length)
        .dropRight(windowProperties.length).mkString(", ")})")
      val propertyOutNames = outFieldNames.slice(
        outFieldNames.length - windowProperties.length,
        outFieldNames.length)
      groupingOutNames ++ aggOutNames ++ propertyOutNames
    } else {
      outFieldNames
    }

    val propStrings = windowProperties.map(_.getProperty.toString)
    (groupingNames ++ aggStrings ++ propStrings).zip(outputFieldNames).map {
      case (f, o) if f == o => f
      case (f, o) => s"$f AS $o"
    }.mkString(", ")
  }

  private def stringifyLocalAggregates(
    aggInfos: Array[AggregateInfoV2],
    distincts: Array[DistinctInfoV2],
    distinctAggs: Map[Int, String],
    aggFilters: Map[Int, Int],
    inFieldNames: Array[String]): Array[String] = {
    val aggStrs = aggInfos.zipWithIndex.map { case (aggInfo, index) =>
      val buf = new mutable.StringBuilder
      buf.append(aggInfo.agg.getAggregation)
      if (aggInfo.consumeRetraction) {
        buf.append("_RETRACT")
      }
      buf.append("(")
      val argNames = aggInfo.agg.getArgList.map(inFieldNames(_))
      if (distinctAggs.contains(index)) {
        buf.append(if (argNames.nonEmpty) s"${distinctAggs(index)} " else distinctAggs(index))
      }
      val argNameStr = if (argNames.nonEmpty) {
        argNames.mkString(", ")
      } else {
        "*"
      }
      buf.append(argNameStr).append(")")
      if (aggFilters(index) >= 0) {
        val filterName = inFieldNames(aggFilters(index))
        buf.append(" FILTER ").append(filterName)
      }
      buf.toString
    }
    val distinctStrs = distincts.map { distinctInfo =>
      val argNames = distinctInfo.argIndexes.map(inFieldNames(_)).mkString(", ")
      s"DISTINCT($argNames)"
    }
    aggStrs ++ distinctStrs
  }

  private def localAggOutputFieldNames(
    aggOffset: Int,
    aggInfos: Array[AggregateInfoV2],
    accNames: Array[String]): Array[String] = {
    var offset = aggOffset
    val aggOutputNames = aggInfos.map { info =>
      info.function match {
        case _: AggregateFunction[_, _] =>
          val name = accNames(offset)
          offset = offset + 1
          name
        case daf: DeclarativeAggregateFunction =>
          val aggBufferTypes = daf.getAggBufferTypes.map(_.getLogicalType)
          val name = aggBufferTypes.indices
            .map(i => accNames(offset + i))
            .mkString(", ")
          offset = offset + aggBufferTypes.length
          if (aggBufferTypes.length > 1) s"($name)" else name
        case _ =>
          throw new TableException(s"Unsupported function: ${info.function}")
      }
    }
    val distinctFieldNames = (offset until accNames.length).map(accNames)
    aggOutputNames ++ distinctFieldNames
  }
  private def stringifyGlobalAggregates(
    aggInfos: Array[AggregateInfoV2],
    distinctAggs: Map[Int, String],
    accFieldNames: Seq[String]): Array[String] = {
    aggInfos.zipWithIndex.map { case (aggInfo, index) =>
      val buf = new mutable.StringBuilder
      buf.append(aggInfo.agg.getAggregation)
      if (aggInfo.consumeRetraction) {
        buf.append("_RETRACT")
      }
      buf.append("(")
      if (index >= accFieldNames.length) {
        println()
      }
      val argNames = accFieldNames(index)
      if (distinctAggs.contains(index)) {
        buf.append(s"${distinctAggs(index)} ")
      }
      buf.append(argNames).append(")")
      buf.toString
    }
  }

  private def stringifyAggregates(
    aggInfos: Array[AggregateInfoV2],
    distinctAggs: Map[Int, String],
    aggFilters: Map[Int, Int],
    inFields: Array[String]): Array[String] = {
    // MAX_RETRACT(DISTINCT a) FILTER b
    aggInfos.zipWithIndex.map { case (aggInfo, index) =>
      val buf = new mutable.StringBuilder
      buf.append(aggInfo.agg.getAggregation)
      if (aggInfo.consumeRetraction) {
        buf.append("_RETRACT")
      }
      buf.append("(")
      val argNames = aggInfo.agg.getArgList.map(inFields(_))
      if (distinctAggs.contains(index)) {
        buf.append(if (argNames.nonEmpty) "DISTINCT " else "DISTINCT")
      }
      val argNameStr = if (argNames.nonEmpty) {
        argNames.mkString(", ")
      } else {
        "*"
      }
      buf.append(argNameStr).append(")")
      if (aggFilters(index) >= 0) {
        val filterName = inFields(aggFilters(index))
        buf.append(" FILTER ").append(filterName)
      }
      buf.toString
    }
  }
}
