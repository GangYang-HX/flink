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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.hint.RelHint
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.hint.FlinkHints.LATENCY_JOIN_TYPE
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecLatencyJoinV2
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, IntervalJoinUtil}
import org.apache.flink.table.runtime.operators.join.latency.v2.config.{LatencyJoinConfig, LatencyJoinConfiguration}

import java.util
import scala.collection.JavaConversions._
import scala.util.control.Breaks

/**
 * Rule that converts non-SEMI/ANTI [[FlinkLogicalJoin]] with window bounds in join condition
 * to [[StreamExecLatencyJoinV2]].
 */
class StreamExecLatencyJoinV2Rule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecLatencyJoinV2Rule") {

  def containsLatencyJoinTypeV2Hint(hint: RelHint): Boolean = {
    if (hint.inheritPath.isEmpty
      && hint.hintName == LATENCY_JOIN_TYPE
      && !hint.listOptions.isEmpty
      && hint.listOptions.get(0).toLowerCase() == "v2") {
      true
    }
    else false
  }

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0)
    val joinRowType = join.getRowType
    val joinInfo = join.analyzeCondition()

    var isLatencyJoin = false
    val joinHints = join.getHints
    val loop = new Breaks
    loop.breakable {
      for (hint <- joinHints) {
        if (containsLatencyJoinTypeV2Hint(hint)) {
          isLatencyJoin = true
          loop.break
        }
      }
    }

    if (!isLatencyJoin) {
      return false
    }

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
    val join = rel.asInstanceOf[FlinkLogicalJoin]
    val joinRowType = join.getRowType
    val left = join.getLeft
    val right = join.getRight

    def toHashTraitByColumns(
                              columns: util.Collection[_ <: Number],
                              inputTraitSet: RelTraitSet) = {
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

    var joinMergeEnable = tableConfig.getConfiguration.getBoolean(LatencyJoinConfiguration.JOIN_LATENCY_MERGE_ENABLE_CONFIG)
    var joinMergeDelimiter = tableConfig.getConfiguration.getString(LatencyJoinConfiguration.JOIN_LATENCY_MERGE_DELIMITER_CONFIG)
    var isDistinctJoinType = tableConfig.getConfiguration.getBoolean(LatencyJoinConfiguration.JOIN_LATENCY_DISTINCT_CONFIG)
    var isTtlEnable = tableConfig.getConfiguration.getBoolean(LatencyJoinConfiguration.JOIN_LATENCY_TTL_ENABLE_CONFIG)
    var cleanupInterval = tableConfig.getConfiguration.getInteger(LatencyJoinConfiguration.JOIN_LATENCY_CLEANUP_INTERVAL_CONFIG)
    var isGlobalJoinType = true

    val hints = join.getHints
    if (hints.size > 0) {
      val joinHintInfo = hints.get(0).kvOptions
      if (joinHintInfo.containsKey("merge")) {
        if (joinHintInfo.get("merge").toLowerCase == "true") {
          joinMergeEnable = true
        }
        else if (joinHintInfo.get("merge").toLowerCase == "false") {
          joinMergeEnable = false
        }
      }

      if (joinHintInfo.containsKey("delimiter")) {
        joinMergeDelimiter = joinHintInfo.get("delimiter").toLowerCase
      }

      if (joinHintInfo.containsKey("distinct")) {
        if (joinHintInfo.get("distinct").toLowerCase == "true") {
          isDistinctJoinType = true
        }
        else if (joinHintInfo.get("distinct").toLowerCase == "false") {
          isDistinctJoinType = false
        }
      }

      if (joinHintInfo.containsKey("global")) {
        if (joinHintInfo.get("global").toLowerCase == "true") {
          isGlobalJoinType = true
        }
        else if (joinHintInfo.get("global").toLowerCase == "false") {
          isGlobalJoinType = false
        }
      }

      if (joinHintInfo.containsKey("ttlEnable")) {
        if (joinHintInfo.get("ttlEnable").toLowerCase == "true") {
          isTtlEnable = true
        }
        else if (joinHintInfo.get("ttlEnable").toLowerCase == "false") {
          isTtlEnable = false
        }
      }
    }

    val joinConfig = new LatencyJoinConfig

    joinConfig.setJoinMergeDelimiter(joinMergeDelimiter)
    joinConfig.setJoinMergeEnable(joinMergeEnable)
    joinConfig.setDistinctJoin(isDistinctJoinType)
    joinConfig.setGlobalJoin(isGlobalJoinType)
    joinConfig.setTtlEnable(isTtlEnable)
    joinConfig.setCleanUpInterval(cleanupInterval)

    new StreamExecLatencyJoinV2(
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
      remainCondition,
      joinConfig)
  }
}

object StreamExecLatencyJoinV2Rule {
  val INSTANCE: RelOptRule = new StreamExecLatencyJoinV2Rule
}
