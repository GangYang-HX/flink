package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.transformations.TwoInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.planner.plan.utils.{IntervalJoinUtil, JoinTypeUtil, KeySelectorUtil}
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.runtime.operators.join.latency.config.GlobalJoinConfig
import org.apache.flink.table.runtime.operators.join.latency.{ProcTimeGlobalJoin, ProcTimeGlobalJoinLocal}
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import java.util
import scala.collection.JavaConversions._

class StreamExecLatencyJoin(
                            cluster: RelOptCluster,
                            traitSet: RelTraitSet,
                            leftRel: RelNode,
                            rightRel: RelNode,
                            override val joinCondition: RexNode,
                            override val joinType: JoinRelType,
                            outputRowType: RelDataType,
                            override val isRowTime: Boolean,
                            config: GlobalJoinConfig,
                            leftTimeIndex: Int,
                            rightTimeIndex: Int,
                            remainCondition: Option[RexNode])
  extends StreamExecIntervalJoin(cluster, traitSet, leftRel, rightRel, joinCondition, joinType, outputRowType,
    isRowTime, 0, config.getDelayMs, leftTimeIndex, rightTimeIndex, null, remainCondition)
    with StreamPhysicalRel
    with StreamExecNode[RowData] {

  if (containsPythonCall(remainCondition.get)) {
    throw new TableException("Only inner join condition with equality predicates supports the " +
      "Python UDF taking the inputs from the left table and the right table at the same time, " +
      "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  // TODO remove FlinkJoinType
  private lazy val flinkJoinType: FlinkJoinType = JoinTypeUtil.getFlinkJoinType(joinType)

  override def requireWatermark: Boolean = isRowTime

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecLatencyJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinType,
      outputRowType,
      isRowTime,
      config,
      leftTimeIndex,
      rightTimeIndex,
      remainCondition)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val windowBounds = s"isRowTime=$isRowTime , leftTimeIndex=$leftTimeIndex, " +
      s"rightTimeIndex=$rightTimeIndex"
    super.explainTerms(pw)
      .item("joinType", "latency-" + flinkJoinType.toString)
      .item("windowBounds", windowBounds)
      .item("where", getExpressionString(
        joinCondition, outputRowType.getFieldNames.toList, None, preferExpressionFormat(pw)))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
                                 ordinalInParent: Int, newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
                                                  planner: StreamPlanner): Transformation[RowData] = {
    val leftPlan = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val rightPlan = getInputNodes.get(1).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    flinkJoinType match {
      case FlinkJoinType.INNER |
           FlinkJoinType.LEFT |
           FlinkJoinType.RIGHT |
           FlinkJoinType.FULL =>
        val leftRowType = FlinkTypeFactory.toLogicalRowType(getLeft.getRowType)
        val rightRowType = FlinkTypeFactory.toLogicalRowType(getRight.getRowType)
        val returnType = RowDataTypeInfo.of(
          FlinkTypeFactory.toLogicalRowType(getRowType))

        // get the equi-keys and other conditions
        //todo: make sure only one equal condition is set
        val joinInfo = JoinInfo.of(left, right, joinCondition)
        val leftKeys = joinInfo.leftKeys.toIntArray
        val rightKeys = joinInfo.rightKeys.toIntArray

        // generate join function
        val joinFunction = IntervalJoinUtil.generateJoinFunction(
          planner.getTableConfig,
          joinType,
          leftRowType,
          rightRowType,
          getRowType,
          remainCondition,
          "LatencyJoinFunction")

        createProcTimeJoin(
          leftPlan,
          rightPlan,
          returnType,
          joinFunction,
          leftKeys,
          rightKeys)

      case FlinkJoinType.ANTI =>
        throw new TableException(
          "Interval Join: {Anti Join} between stream and stream is not supported yet.\n" +
            "please re-check latency join statement according to description above.")
      case FlinkJoinType.SEMI =>
        throw new TableException(
          "Interval Join: {Semi Join} between stream and stream is not supported yet.\n" +
            "please re-check latency join statement according to description above.")
    }
  }

  private def createProcTimeJoin(
                                  leftPlan: Transformation[RowData],
                                  rightPlan: Transformation[RowData],
                                  returnTypeInfo: RowDataTypeInfo,
                                  joinFunction: GeneratedFunction[FlatJoinFunction[RowData, RowData, RowData]],
                                  leftKeys: Array[Int],
                                  rightKeys: Array[Int]): Transformation[RowData] = {

    assert(leftKeys.length == 1, "latency join only support one key")
    val leftTypeInfo = leftPlan.getOutputType.asInstanceOf[RowDataTypeInfo]
    val rightTypeInfo = rightPlan.getOutputType.asInstanceOf[RowDataTypeInfo]
    // set KeyType and Selector for state
    val leftSelect = KeySelectorUtil.getRowDataSelector(leftKeys, leftTypeInfo)
    val rightSelect = KeySelectorUtil.getRowDataSelector(rightKeys, rightTypeInfo)

    val procJoinFunc = {
      // JoinClass 在 runapp 分支已弃用，原有redisJoinWithTTL 逻辑已在ProcTimeLatencyJoin中兼容
      //如果JoinClass没有值，走老逻辑
      if ("rocksdb".equalsIgnoreCase(config.getRedisAddress))
        new ProcTimeGlobalJoinLocal(
          flinkJoinType,
          config,
          leftTypeInfo,
          rightTypeInfo,
          returnTypeInfo,
          joinFunction,
          leftSelect,
          rightSelect) else
        new ProcTimeGlobalJoin(
          flinkJoinType,
          config,
          leftTypeInfo,
          rightTypeInfo,
          returnTypeInfo,
          joinFunction,
          leftSelect,
          rightSelect)
    }


    val ret = new TwoInputTransformation[RowData, RowData, RowData](leftPlan, rightPlan, getRelDetailedDescription, getFlinkRelTypeName, new KeyedCoProcessOperator(procJoinFunc).
            asInstanceOf[TwoInputStreamOperator[RowData, RowData, RowData]], returnTypeInfo, leftPlan.getParallelism)

    if (leftKeys.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.getProducedType)
    ret
  }


}
