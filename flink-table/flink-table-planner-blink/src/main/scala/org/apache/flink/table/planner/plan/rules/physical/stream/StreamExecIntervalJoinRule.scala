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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecIntervalJoin
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, IntervalJoinUtil}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.runtime.operators.join.interval.config.{IntervalJoinConfiguration, IntervalJoinConfig}

import java.util

import scala.collection.JavaConversions._

/**
  * Rule that converts non-SEMI/ANTI [[FlinkLogicalJoin]] with window bounds in join condition
  * to [[StreamExecIntervalJoin]].
  */
class StreamExecIntervalJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecIntervalJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0)
    val joinRowType = join.getRowType
    val joinInfo = join.analyzeCondition()

    // joins require an equi-condition or a conjunctive predicate with at least one equi-condition
    // TODO support SEMI/ANTI join
    if (!join.getJoinType.projectsRight || joinInfo.pairs().isEmpty) {
      return false
    }

    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(join)
    val (windowBounds, _) = IntervalJoinUtil.extractWindowBoundsFromPredicate(
      join.getCondition,
      join.getLeft.getRowType.getFieldCount,
      joinRowType,
      join.getCluster.getRexBuilder,
      tableConfig)

    if (windowBounds.isDefined) {
      if (windowBounds.get.isEventTime) {
        true
      } else {
        // Check that no event-time attributes are in the input because the processing time window
        // join does not correctly hold back watermarks.
        // We rely on projection pushdown to remove unused attributes before the join.
        !joinRowType.getFieldList.exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
      }
    } else {
      // the given join does not have valid window bounds. We cannot translate it.
      false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val join: FlinkLogicalJoin = rel.asInstanceOf[FlinkLogicalJoin]
    val joinRowType = join.getRowType
    val left = join.getLeft
    val right = join.getRight

    def toHashTraitByColumns(
        columns: util.Collection[_ <: Number],
        inputTraitSet: RelTraitSet): RelTraitSet = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSet
        .replace(FlinkConventions.STREAM_PHYSICAL)
        .replace(distribution)
    }

    val joinInfo = join.analyzeCondition
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, right.getTraitSet))

    val newLeft = RelOptRule.convert(left, leftRequiredTrait)
    val newRight = RelOptRule.convert(right, rightRequiredTrait)
    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val tableConfig = rel
      .getCluster
      .getPlanner
      .getContext
      .unwrap(classOf[FlinkContext])
      .getTableConfig
    val (windowBounds, remainCondition) = IntervalJoinUtil.extractWindowBoundsFromPredicate(
      join.getCondition,
      left.getRowType.getFieldCount,
      joinRowType,
      join.getCluster.getRexBuilder,
      tableConfig)

    val CleanupInterval = tableConfig.getConfiguration.getInteger(IntervalJoinConfiguration.INTERVAL_CLEANUP_INTERVAL_CONFIG)

    val config = new IntervalJoinConfig

    config.setCleanUpInterval(CleanupInterval)

    new StreamExecIntervalJoin(
      rel.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getCondition,
      join.getJoinType,
      joinRowType,
      windowBounds.get.isEventTime,
      windowBounds.get.leftLowerBound,
      windowBounds.get.leftUpperBound,
      windowBounds.get.leftTimeIdx,
      windowBounds.get.rightTimeIdx,
      config,
      remainCondition)
  }
}

object StreamExecIntervalJoinRule {
  val INSTANCE: RelOptRule = new StreamExecIntervalJoinRule
}
