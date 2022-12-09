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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.{PlannerBase, StreamPlanner}
import org.apache.flink.table.planner.plan.logical.WindowingStrategy
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.WindowUtil.checkEmitConfiguration
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Streaming window aggregate physical node which will be translate to window aggregate operator.
 *
 * Note: The differences between [[StreamPhysicalWindowAggregate]] and
 * [[StreamPhysicalGroupWindowAggregate]] is that, [[StreamPhysicalWindowAggregate]] is translated
 * from window TVF syntax, but the other is from the legacy GROUP WINDOW FUNCTION syntax.
 * In the long future, [[StreamPhysicalGroupWindowAggregate]] will be dropped.
 */
class StreamPhysicalWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    val grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val windowing: WindowingStrategy,
    val namedWindowProperties: Seq[NamedWindowProperty])
  extends SingleRel(cluster, traitSet, inputRel)
    with StreamPhysicalRel
    with StreamExecNode[RowData]{

  lazy val aggInfoList: AggregateInfoListV2 = AggregateUtil.deriveStreamWindowAggregateInfoList(
    FlinkTypeFactory.toLogicalRowType(inputRel.getRowType),
    aggCalls,
    windowing.getWindow,
    isStateBackendDataViews = true)


  override def copy(
    traitSet: RelTraitSet,
    inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalWindowAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      grouping,
      aggCalls,
      windowing,
      namedWindowProperties
    )
  }

  /**
   * Whether the [[StreamPhysicalRel]] requires rowtime watermark in processing logic.
   */
  override def requireWatermark: Boolean = windowing.isRowtime

  /**
   * Returns an array of this node's inputs. If there are no inputs,
   * returns an empty list, not null.
   *
   * @return Array of this node's inputs
   */
  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  /**
   * Replaces the <code>ordinalInParent</code><sup>th</sup> input.
   * You must override this method if you override [[getInputNodes]].
   *
   * @param ordinalInParent Position of the child input, 0 is the first
   * @param newInputNode    New node that should be put at position ordinalInParent
   */
  override def replaceInputNode(ordinalInParent: Int, newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def deriveRowType(): RelDataType = {
    WindowUtil.deriveWindowAggregateRowType(
      grouping,
      aggCalls,
      windowing,
      namedWindowProperties,
      inputRel.getRowType,
      cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    val inputFieldNames = inputRowType.getFieldNames.asScala.toArray
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("window", windowing.toSummaryString(inputFieldNames))
      .item("select", RelExplainUtilV2.streamWindowAggregationToString(
        inputRowType,
        getRowType,
        aggInfoList,
        grouping,
        namedWindowProperties))
  }

  /**
   * Internal method, translates this node into a Flink operator.
   *
   * @param planner The [[Planner]] of the translated Table.
   */
  override protected def translateToPlanInternal(planner: StreamPlanner): Transformation[RowData] = {
    val input = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    checkEmitConfiguration(FlinkRelOptUtil.getTableConfigFromContext(this))
    val aggregate = new StreamExecWindowAggregate(
      input,
      grouping,
      aggCalls.toArray,
      windowing,
      namedWindowProperties.toArray,
      RowDataTypeInfo.of(
        FlinkTypeFactory.toLogicalRowType(getRowType)),
      getRelDetailedDescription)
    val config = planner.asInstanceOf[PlannerBase].getTableConfig
    aggregate.translateToPlanInternal(planner.asInstanceOf[PlannerBase], config)
  }
}
