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

import org.apache.calcite.plan.{RelOptRule, RelOptTable}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.common.CommonKeyedLookupJoin
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecKeyedLookupJoin
import org.apache.flink.table.planner.plan.rules.physical.common.{BaseKeyedSnapshotOnCalcTableScanRule, BaseKeyedSnapshotOnTableScanRule}

/**
  * Rules that convert [[FlinkLogicalJoin]] on a [[FlinkLogicalSnapshot]]
  * into [[StreamExecKeyedLookupJoin]]
  *
  * There are 2 conditions for this rule:
  * 1. the root parent of [[FlinkLogicalSnapshot]] should be a TableSource which implements
  *   [[org.apache.flink.table.sources.LookupableTableSource]].
  * 2. the period of [[FlinkLogicalSnapshot]] must be left table's proctime attribute.
  */
object StreamExecKeyedLookupJoinRule {
  val SNAPSHOT_ON_KEYED_TABLESCAN: RelOptRule = new KeyedSnapshotOnTableScanRule
  val SNAPSHOT_ON_KEYED_CALC_TABLESCAN: RelOptRule = new KeyedSnapshotOnCalcTableScanRule

  class KeyedSnapshotOnTableScanRule
    extends BaseKeyedSnapshotOnTableScanRule("StreamExecKeyedSnapshotOnTableScanRule") {

    override protected def transform(
        join: FlinkLogicalJoin,
        input: FlinkLogicalRel,
        temporalTable: RelOptTable,
        calcProgram: Option[RexProgram]): CommonKeyedLookupJoin = {
      doTransform(join, input, temporalTable, calcProgram)
    }
  }

  class KeyedSnapshotOnCalcTableScanRule
    extends BaseKeyedSnapshotOnCalcTableScanRule("StreamExecKeyedSnapshotOnCalcTableScanRule") {

    override protected def transform(
        join: FlinkLogicalJoin,
        input: FlinkLogicalRel,
        temporalTable: RelOptTable,
        calcProgram: Option[RexProgram]): CommonKeyedLookupJoin = {
      doTransform(join, input, temporalTable, calcProgram)
    }
  }

  private def doTransform(
    join: FlinkLogicalJoin,
    input: FlinkLogicalRel,
    temporalTable: RelOptTable,
    calcProgram: Option[RexProgram]): StreamExecKeyedLookupJoin = {

    val joinInfo = join.analyzeCondition

    val cluster = join.getCluster

    val providedTrait = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val requiredTrait = if(joinInfo.leftKeys.size() == 0){
      input.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    } else {
      val distribution = FlinkRelDistribution.hash(joinInfo.leftKeys)
      input.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL).replace(distribution)
    }

    val convInput = RelOptRule.convert(input, requiredTrait)
    new StreamExecKeyedLookupJoin(
      cluster,
      providedTrait,
      convInput,
      temporalTable,
      calcProgram,
      joinInfo,
      join.getJoinType)
  }
}
