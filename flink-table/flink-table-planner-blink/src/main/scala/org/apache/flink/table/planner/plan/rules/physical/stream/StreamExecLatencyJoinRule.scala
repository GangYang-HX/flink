package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.hint.FlinkHints.LATENCY_JOIN_TYPE
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecLatencyJoin
import org.apache.flink.table.planner.plan.utils.{ExpandUtil, FlinkRelOptUtil, IntervalJoinUtil}
import org.apache.flink.table.runtime.operators.join.latency.config.{GlobalConfiguration, GlobalJoinConfig}

import java.util
import scala.collection.JavaConversions._
import scala.util.control.Breaks


class StreamExecLatencyJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamExecLatencyJoinRule") {

  def containsLatencyJoinTypeV1Hint(hint: RelHint): Boolean = {
    if (hint.inheritPath.isEmpty
      && hint.hintName == LATENCY_JOIN_TYPE
      && !hint.listOptions.isEmpty
      && hint.listOptions.get(0).toLowerCase == "v1") {
      true
    }
    else false
  }

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    val logicalRel = call.rel[FlinkLogicalRel](2)

    val joinRowType = join.getRowType

    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(join)
    val (windowBoundsLeft, windowBoundsRight, _) = IntervalJoinUtil.extractLatencyJoinPeriodFromPredicate(
      join.getCondition,
      join.getLeft.getRowType.getFieldCount,
      joinRowType,
      join.getCluster.getRexBuilder,
      tableConfig)

    if (windowBoundsLeft.isDefined && windowBoundsRight.isDefined) {
      if (!windowBoundsRight.get.isEventTime && !windowBoundsLeft.get.isEventTime) {
        true
      } else {
        throw new UnsupportedOperationException("latency join only happens on the proctime fields")
      }
    } else {
      false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: FlinkLogicalJoin = call.rel(0)
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

    val joinInfo = join.analyzeCondition()
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, right.getTraitSet))

    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val newLeft: RelNode = RelOptRule.convert(left, leftRequiredTrait)
    val newRight: RelNode = RelOptRule.convert(right, rightRequiredTrait)

    val tableConfig = join
      .getCluster
      .getPlanner
      .getContext
      .unwrap(classOf[FlinkContext])
      .getTableConfig

    val (windowBoundsLeft, windowBoundsRight, remainCondition) = IntervalJoinUtil.extractLatencyJoinPeriodFromPredicate(
      join.getCondition,
      join.getLeft.getRowType.getFieldCount,
      joinRowType,
      join.getCluster.getRexBuilder,
      tableConfig)

    /**
     * left and right bound should have different index
     * */
    assert(windowBoundsLeft.get.leftTimeIdx == windowBoundsRight.get.rightTimeIdx)
    assert(windowBoundsLeft.get.rightTimeIdx == windowBoundsRight.get.leftTimeIdx)
    assert(windowBoundsLeft.get.leftLowerBound < 0
      && windowBoundsLeft.get.leftUpperBound == 0,
      "make sure latency join the left value between right - interval and right")
    assert(windowBoundsRight.get.leftLowerBound == 0
      && windowBoundsRight.get.leftUpperBound > 0,
      "make sure latency join the right value between left - interval and left")

    val redisAddr = tableConfig.getConfiguration.getString(GlobalConfiguration.LATENCY_REDIS_CONFIG)
    val redisType = tableConfig.getConfiguration.getInteger(GlobalConfiguration.LATENCY_REDIS_TYPE_CONFIG)
    val compressType = tableConfig.getConfiguration.getInteger(GlobalConfiguration.LATENCY_COMPRESS_TYPE_CONFIG)
    val redisBucketSize = tableConfig.getConfiguration.getInteger(GlobalConfiguration.LATENCY_REDIS_BUCKET_SIZE_CONFIG)
    val maxCacheSize = tableConfig.getConfiguration.getInteger(GlobalConfiguration.LATENCY_MAX_CACHE_SIZE_CONFIG)
    assert(redisAddr != null, "latency join need to make sure the redis is not null")
    val joinMergeFunction = tableConfig.getConfiguration.getString(GlobalConfiguration.LATENCY_MERGE_FUNCTION_CONFIG)
    val latencyjoinSaveKey = tableConfig.getConfiguration.getBoolean(GlobalConfiguration.LATENCY_SAVE_KEY_CONFIG)
    val stateTtl = tableConfig.getMinIdleStateRetentionTime
    val keyedStateTtlEnable = ExpandUtil.keyedStateTtlEnable(tableConfig.getConfiguration)

    val latencyInterval = -windowBoundsLeft.get.leftLowerBound
    val windowInterval = windowBoundsRight.get.leftUpperBound
    var globalJoin = false

    val joinHints = join.getHints
    val loop = new Breaks
    loop.breakable {
      for (hint <- joinHints) {
        if (containsLatencyJoinTypeV1Hint(hint)) {
          globalJoin = true
          loop.break
        }
      }
    }

    val config = new GlobalJoinConfig
    //todo: set jobId/ joinNo/ merge
    config.setDelayMs(latencyInterval)
    config.setWindowLengthMs(windowInterval)
    config.setBucketNum(redisBucketSize)
    config.setRedisType(redisType)
    config.setCompressType(compressType)
    config.setGlobalLatency(globalJoin)
    config.setRedisAddress(redisAddr)
    config.setMaxCacheSize(maxCacheSize)
    config.setMergeSideFunc(joinMergeFunction)
    config.setSaveTimerKey(latencyjoinSaveKey)
    config.setStateTtl(stateTtl)
    config.setKeyedStateTtlEnable(keyedStateTtlEnable)

    val newJoin = new StreamExecLatencyJoin(
      join.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getCondition,
      join.getJoinType,
      joinRowType,
      windowBoundsLeft.get.isEventTime,
      config,
      windowBoundsLeft.get.leftTimeIdx,
      windowBoundsLeft.get.rightTimeIdx,
      remainCondition)
    call.transformTo(newJoin)

  }
}


object StreamExecLatencyJoinRule {
  val INSTANCE: RelOptRule = new StreamExecLatencyJoinRule
}
