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
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.{PlannerBase, StreamPlanner}
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalWindowTableFunction
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import scala.collection.JavaConversions._

/**
 * Stream physical RelNode for window table-valued function.
 */
class StreamPhysicalWindowTableFunction(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  inputRel: RelNode,
  outputRowType: RelDataType,
  windowing: TimeAttributeWindowingStrategy)
  extends CommonPhysicalWindowTableFunction(cluster, traitSet, inputRel, outputRowType, windowing)
    with StreamPhysicalRel
    with StreamExecNode[RowData]{


  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalWindowTableFunction(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      windowing)
  }

  /**
   * Whether the [[StreamPhysicalRel]] requires rowtime watermark in processing logic.
   */
  override def requireWatermark: Boolean = true

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


  /**
   * Internal method, translates this node into a Flink operator.
   *
   * @param planner The [[Planner]] of the translated Table.
   */
  override protected def translateToPlanInternal(planner: StreamPlanner): Transformation[RowData] = {
    val inputTransform = getInputNodes.get(0).translateToPlan(planner).asInstanceOf[Transformation[RowData]]
    val aggregate = new StreamExecWindowTableFunction(
      inputTransform,
      unwrapTableConfig(this),
      windowing,
      RowDataTypeInfo.of(
        FlinkTypeFactory.toLogicalRowType(getRowType)),
      getRelDetailedDescription)
    val config = planner.asInstanceOf[PlannerBase].getTableConfig
    aggregate.translateToPlanInternal(planner.asInstanceOf[PlannerBase],config)
  }
}
