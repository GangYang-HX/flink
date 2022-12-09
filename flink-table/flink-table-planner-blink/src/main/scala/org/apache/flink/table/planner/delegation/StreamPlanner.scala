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

package org.apache.flink.table.planner.delegation

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.api.internal.SelectTableSink
import org.apache.flink.table.api.{ExplainDetail, LineAgeInfo, PhysicalExecutionPlan, TableConfig, TableException, TableSchema}
import org.apache.flink.table.catalog.{CatalogManager, CatalogTable, CatalogTableImpl, FunctionCatalog, ObjectIdentifier}
import org.apache.flink.table.delegation.Executor
import org.apache.flink.table.operations.{CatalogSinkModifyOperation, ModifyOperation, Operation, QueryOperation}
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.optimize.{Optimizer, StreamCommonSubGraphBasedOptimizer}
import org.apache.flink.table.planner.plan.utils.{ExecNodePlanDumper, FlinkRelOptUtil}
import org.apache.flink.table.planner.sinks.StreamSelectTableSink
import org.apache.flink.table.planner.utils.{DummyStreamExecutionEnvironment, ExecutorUtils, JavaScalaConversionUtil, Logging, PlanUtil}
import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.rel.logical.{LogicalJoin, LogicalSnapshot, LogicalTableModify, LogicalTableScan}
import org.apache.calcite.sql.SqlExplainLevel
import java.util

import org.apache.calcite.rel.RelNode
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api.LineAgeInfo.{LineAgeInfoBuilder, TableType}
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalLegacySink
import org.apache.flink.table.planner.plan.schema.TableSourceTable

import _root_.scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class StreamPlanner(
                     executor: Executor,
                     config: TableConfig,
                     functionCatalog: FunctionCatalog,
                     catalogManager: CatalogManager)
  extends PlannerBase(executor, config, functionCatalog, catalogManager, isStreamingMode = true)
    with Logging {

  override protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      MiniBatchIntervalTraitDef.INSTANCE,
      ModifyKindSetTraitDef.INSTANCE,
      UpdateKindTraitDef.INSTANCE)
  }

  override protected def getOptimizer: Optimizer = new StreamCommonSubGraphBasedOptimizer(this)

  override protected def translateToPlan(
                                          execNodes: util.List[ExecNode[_, _]]): util.List[Transformation[_]] = {
    val planner = createDummyPlanner()
    planner.overrideEnvParallelism()

    execNodes.map {
      case node: StreamExecNode[_] => node.translateToPlan(planner)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  override def createSelectTableSink(tableSchema: TableSchema): SelectTableSink = {
    new StreamSelectTableSink(tableSchema)
  }

  def getStreamGraph(operations: util.List[Operation]): StreamGraph = {
    require(operations.nonEmpty, "operations should not be empty")
    val filterOps = operations.filter((operation: Operation) => operation.isInstanceOf[CatalogSinkModifyOperation]).toList
    val sinkRelNodes = filterOps.map {
      case queryOperation: QueryOperation =>
        val relNode = getRelBuilder.queryOperation(queryOperation).build()
        relNode match {
          // SQL: explain plan for insert into xx
          case modify: LogicalTableModify =>
            // convert LogicalTableModify to CatalogSinkModifyOperation
            val qualifiedName = modify.getTable.getQualifiedName
            require(qualifiedName.size() == 3, "the length of qualified name should be 3.")
            val modifyOperation = new CatalogSinkModifyOperation(
              ObjectIdentifier.of(qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2)),
              new PlannerQueryOperation(modify.getInput)
            )
            translateToRel(modifyOperation)
          case _ =>
            relNode
        }
      case modifyOperation: ModifyOperation =>
        translateToRel(modifyOperation)
      case o => throw new TableException(s"Unsupported operation: ${o.getClass.getCanonicalName}")
    }
    val optimizedRelNodes = optimize(sinkRelNodes)
    val execNodes = translateToExecNodePlan(optimizedRelNodes)

    val transformations = translateToPlan(execNodes)

    val execEnv = getExecEnv
    ExecutorUtils.setBatchProperties(execEnv, getTableConfig)
    ExecutorUtils.generateStreamGraph(execEnv, transformations)
  }

  override def getPhysicalExecutionPlan(operations: util.List[Operation], extraDetails: ExplainDetail*): util.List[PhysicalExecutionPlan] = {
    val streamGraph = getStreamGraph(operations)
    PlanUtil.generatePhysicalExecutionPlan(streamGraph)
  }

//  override def generateLineAge(operations: util.List[Operation], extraDetails: ExplainDetail*): util.List[LineAgeInfo] = {
//    val streamGraph = getStreamGraph(operations)
//    ExecutorUtils.setBatchProperties(streamGraph, getTableConfig)
//    // FIXME: Currently only one list is returned, saber is enough, consider providing DAG later.
//    val lineAgeInfos = PlanUtil.generateLineAgeFromStreamGraph(streamGraph, operations)
//    lineAgeInfos
//  }

  override def explain(operations: util.List[Operation], extraDetails: ExplainDetail*): String = {
    require(operations.nonEmpty, "operations should not be empty")
    val sinkRelNodes = operations.map {
      case queryOperation: QueryOperation =>
        val relNode = getRelBuilder.queryOperation(queryOperation).build()
        relNode match {
          // SQL: explain plan for insert into xx
          case modify: LogicalTableModify =>
            // convert LogicalTableModify to CatalogSinkModifyOperation
            val qualifiedName = modify.getTable.getQualifiedName
            require(qualifiedName.size() == 3, "the length of qualified name should be 3.")
            val modifyOperation = new CatalogSinkModifyOperation(
              ObjectIdentifier.of(qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2)),
              new PlannerQueryOperation(modify.getInput)
            )
            translateToRel(modifyOperation)
          case _ =>
            relNode
        }
      case modifyOperation: ModifyOperation =>
        translateToRel(modifyOperation)
      case o => throw new TableException(s"Unsupported operation: ${o.getClass.getCanonicalName}")
    }
    val optimizedRelNodes = optimize(sinkRelNodes)
    val execNodes = translateToExecNodePlan(optimizedRelNodes)

    val transformations = translateToPlan(execNodes)
    val streamGraph = ExecutorUtils.generateStreamGraph(getExecEnv, transformations)
    val executionPlan = PlanUtil.explainStreamGraph(streamGraph)

    val sb = new StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkRelNodes.foreach { sink =>
      sb.append(FlinkRelOptUtil.toString(sink))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Logical Plan ==")
    sb.append(System.lineSeparator)
    val explainLevel = if (extraDetails.contains(ExplainDetail.ESTIMATED_COST)) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.DIGEST_ATTRIBUTES
    }
    val withChangelogTraits = extraDetails.contains(ExplainDetail.CHANGELOG_MODE)
    sb.append(ExecNodePlanDumper.dagToString(
      execNodes,
      explainLevel,
      withChangelogTraits = withChangelogTraits))
    sb.append(System.lineSeparator)

    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(executionPlan)
    sb.toString()
  }

  private def createDummyPlanner(): StreamPlanner = {
    val dummyExecEnv = new DummyStreamExecutionEnvironment(getExecEnv)
    val executor = new StreamExecutor(dummyExecEnv)
    new StreamPlanner(executor, config, functionCatalog, catalogManager)
  }

  override def generateLineAge(operations: util.List[Operation], extraDetails: ExplainDetail*): util.List[LineAgeInfo] = {

    require(operations.nonEmpty, "operations should not be empty")

    val lineAgeInfos = ListBuffer[LineAgeInfo]()

    val operationOps = operations.filter((operation: Operation) => operation.isInstanceOf[CatalogSinkModifyOperation]).toList
    val sinkRelNodes = operationOps.map {
      modifyOperation =>
        //sink
        val catalogSinkModifyOperation = modifyOperation.asInstanceOf[CatalogSinkModifyOperation]
        val lineAgeInfoBuilder = LineAgeInfoBuilder.builder()
        val catalogTable = JavaScalaConversionUtil.toScala(catalogManager.getTable(catalogSinkModifyOperation.getTableIdentifier)).map(_.getTable)
        lineAgeInfoBuilder.withCatalogTable(catalogTable.get.asInstanceOf[CatalogTableImpl])
        lineAgeInfoBuilder.withObjectIdentifier(catalogSinkModifyOperation.getTableIdentifier)
        lineAgeInfos += lineAgeInfoBuilder.withTableType(TableType.SINK).build()

        translateToRel(catalogSinkModifyOperation)
    }

    sinkRelNodes.foreach { sinkRelNode =>
      searchSourceTable(lineAgeInfos, sinkRelNode, isSideTable = false)
    }
    lineAgeInfos
  }

  private def searchSourceTable(lineAgeInfos: util.List[LineAgeInfo], relNode: RelNode, isSideTable: Boolean): Unit = {

    relNode match {
      case scan: LogicalTableScan =>
        val tableSourceTable = scan.getTable.asInstanceOf[TableSourceTable]
        val lineAgeInfoBuilder = LineAgeInfoBuilder.builder()
        lineAgeInfoBuilder.withCatalogTable(tableSourceTable.catalogTable)
        lineAgeInfoBuilder.withObjectIdentifier(tableSourceTable.tableIdentifier)
        if (isSideTable) {
          lineAgeInfos += lineAgeInfoBuilder.withTableType(TableType.SIDE).build()
        } else {
          lineAgeInfos += lineAgeInfoBuilder.withTableType(TableType.SOURCE).build()
        }
      case snapshot: LogicalSnapshot =>
        for (inputRelNode <- relNode.asInstanceOf[RelNode].getInputs) {
          searchSourceTable(lineAgeInfos, inputRelNode, isSideTable = true)
        }
      case _ =>
        for (inputRelNode <- relNode.asInstanceOf[RelNode].getInputs) {
          searchSourceTable(lineAgeInfos, inputRelNode, isSideTable)
        }
    }
  }

}
