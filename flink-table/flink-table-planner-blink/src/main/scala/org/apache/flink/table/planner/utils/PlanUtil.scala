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

package org.apache.flink.table.planner.utils

import org.apache.flink.streaming.api.graph.{StreamGraph, StreamNode}
import java.io.{PrintWriter, StringWriter}
import java.util

import org.apache.flink.table.api.{LineAgeInfo, PhysicalExecutionPlan}
import org.apache.flink.table.api.LineAgeInfo.{LineAgeInfoBuilder, TableType}
import org.apache.flink.table.api.PhysicalExecutionPlan.PhysicalExecutionPlanBuilder
import org.apache.flink.table.operations.{CatalogSinkModifyOperation, Operation}
import org.apache.flink.table.operations.ddl.CreateTableOperation

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object PlanUtil extends Logging {

  /**
   * Converting an StreamGraph to a human-readable string.
   *
   * @param graph stream graph
   */
  def explainStreamGraph(graph: StreamGraph): String = {
    def isSource(id: Int): Boolean = graph.getSourceIDs.contains(id)

    def isSink(id: Int): Boolean = graph.getSinkIDs.contains(id)

    // can not convert to single abstract method because it will throw compile error
    implicit val order: Ordering[Int] = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = (isSink(x), isSink(y)) match {
        case (true, false) => 1
        case (false, true) => -1
        case (_, _) => x - y
      }
    }

    val operatorIDs = graph.getStreamNodes.map(_.getId).toList.sorted(order)
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)

    var tabs = 0
    operatorIDs.foreach { id =>
      val op = graph.getStreamNode(id)
      val (nodeType, content) = if (isSource(id)) {
        tabs = 0
        ("Data Source", op.getOperatorName)
      } else if (isSink(id)) {
        ("Data Sink", op.getOperatorName)
      } else {
        ("Operator", op.getOperatorName)
      }

      pw.append("\t" * tabs).append(s"Stage $id : $nodeType\n")
        .append("\t" * (tabs + 1)).append(s"content : $content\n")

      if (!isSource(id)) {
        val partition = op.getInEdges.head.getPartitioner.toString
        pw.append("\t" * (tabs + 1)).append(s"ship_strategy : $partition\n")
      }

      pw.append("\t" * (tabs + 1)).append(s"slot_group : ${op.getSlotSharingGroup}\n")

      pw.append("\n")
      tabs += 1
    }

    pw.close()
    sw.toString
  }

  def generatePhysicalExecutionPlan(graph: StreamGraph): util.List[PhysicalExecutionPlan] = {
    def isSource(id: Int): Boolean = graph.getSourceIDs.contains(id)

    def isSink(id: Int): Boolean = graph.getSinkIDs.contains(id)

    // can not convert to single abstract method because it will throw compile error
    implicit val order: Ordering[Int] = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = (isSink(x), isSink(y)) match {
        case (true, false) => 1
        case (false, true) => -1
        case (_, _) => x - y
      }
    }

    val operatorIDs = graph.getStreamNodes.map(_.getId).toList.sorted(order)
    val physicalExecutionPlans = ListBuffer[PhysicalExecutionPlan]()
    operatorIDs.foreach { id =>
      val op = graph.getStreamNode(id)
      val (nodeType, content) = if (isSource(id)) {
        ("Data Source", op.getOperatorName)
      } else if (isSink(id)) {
        ("Data Sink", op.getOperatorName)
      } else {
        ("Operator", op.getOperatorName)
      }

      val physicalExecutionPlanBuilder = buildPhysicalExecutionPlan(op.getOperatorName, id, nodeType, content)
      if (!isSource(id)) {
        val partition = op.getInEdges.head.getPartitioner.toString
        physicalExecutionPlanBuilder.withShipStrategy(partition)
      }
      physicalExecutionPlans += physicalExecutionPlanBuilder.build()
    }
    physicalExecutionPlans
  }

  def buildPhysicalExecutionPlan(operatorName: String, id: Int, nodeType: String, content: String): PhysicalExecutionPlanBuilder = {
    PhysicalExecutionPlan.PhysicalExecutionPlanBuilder.builder()
      .withOperatorName(operatorName).withId(id).withContent(content).withNodeType(nodeType)
  }

  def generateLineAgeFromStreamGraph(graph: StreamGraph, operations: util.List[Operation]): util.List[LineAgeInfo] = {
    def isSource(id: Int): Boolean = graph.getSourceIDs.contains(id)

    def isSink(id: Int): Boolean = graph.getSinkIDs.contains(id)

    def isSide(operatorName: String): Boolean = operatorName.contains("LookupJoin")

    // can not convert to single abstract method because it will throw compile error
    implicit val order: Ordering[Int] = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = (isSink(x), isSink(y)) match {
        case (true, false) => 1
        case (false, true) => -1
        case (_, _) => x - y
      }
    }

    val operatorIDs = graph.getStreamNodes.map(_.getId).toList.sorted(order)
    val createTableOperation = operations.filter((operation: Operation) => operation.isInstanceOf[CreateTableOperation])
    val catalogSinkModifyOperation = operations.filter((operation: Operation) => operation.isInstanceOf[CatalogSinkModifyOperation])

    val lineAgeInfos = ListBuffer[LineAgeInfo]()
    operatorIDs.foreach { id =>
      val op = graph.getStreamNode(id)
      val (nodeType, content) = if (isSource(id)) {
        ("Data Source", op.getOperatorName)
      } else if (isSink(id)) {
        ("Data Sink", op.getOperatorName)
      } else {
        ("Operator", op.getOperatorName)
      }

      var lineAgeInfoBuilder = generateSourceSideLineAgeInfoBuilder(createTableOperation, content)
      if (isSource(id)) {
        lineAgeInfos += lineAgeInfoBuilder.withTableType(TableType.SOURCE).build()
      } else if (isSide(op.getOperatorName)) {
        lineAgeInfos += lineAgeInfoBuilder.withTableType(TableType.SIDE).build()
      } else if (isSink(id)) {
        if (catalogSinkModifyOperation.nonEmpty) {
          lineAgeInfoBuilder = generateSinkLineAgeInfoBuilder(createTableOperation, catalogSinkModifyOperation, op.getOperatorName)
        }
        lineAgeInfos += lineAgeInfoBuilder.withTableType(TableType.SINK).build()
      }
    }
    lineAgeInfos
  }

  def generateSinkLineAgeInfoBuilder(createTableOperation: util.List[Operation], catalogSinkModifyOperation: util.List[Operation], content: String): LineAgeInfoBuilder = {
    val lineAgeInfoBuilder = LineAgeInfoBuilder.builder()
    var catalog = true
    for (ops <- catalogSinkModifyOperation if catalog) {
      val catalogSinkModifyOps = ops.asInstanceOf[CatalogSinkModifyOperation]
      val catalogCreateTableOps = findOperationFromSinkModifyOperation(createTableOperation, catalogSinkModifyOps)
      if (catalogCreateTableOps.isDefined) {
        fillLineAgeInfoBuilder(content, catalogCreateTableOps.get, lineAgeInfoBuilder)
        catalogSinkModifyOperation -= catalogSinkModifyOps
        catalog = false
      }
    }
    lineAgeInfoBuilder
  }

  def generateSourceSideLineAgeInfoBuilder(createTableOperation: util.List[Operation], content: String): LineAgeInfoBuilder = {
    val lineAgeInfoBuilder = LineAgeInfoBuilder.builder()
    var create = true
    for (ops <- createTableOperation if create) {
      val createTableOps = ops.asInstanceOf[CreateTableOperation]
      val tableName = createTableOps.getTableIdentifier.getObjectName
      val dataBaseName = createTableOps.getTableIdentifier.getDatabaseName
      if (content.contains(tableName) && content.contains(dataBaseName)) {
        fillLineAgeInfoBuilder(content, createTableOps, lineAgeInfoBuilder)
        create = false
      }
    }
    lineAgeInfoBuilder
  }

  def fillLineAgeInfoBuilder(content: String, operation: CreateTableOperation, lineAgeInfoBuilder: LineAgeInfoBuilder): LineAgeInfoBuilder = {
    lineAgeInfoBuilder.withContent(content)
    lineAgeInfoBuilder.withCatalogTable(operation.getCatalogTable)
    lineAgeInfoBuilder.withObjectIdentifier(operation.getTableIdentifier)
  }

  def findOperationFromSinkModifyOperation(createTableOperation: util.List[Operation], catalogsSink: CatalogSinkModifyOperation): Option[CreateTableOperation] = {
    for (ops <- createTableOperation) {
      val createTableOps = ops.asInstanceOf[CreateTableOperation]
      val createTableIde = createTableOps.getTableIdentifier
      val catalogSinkTableIde = catalogsSink.getTableIdentifier
      if (createTableIde.getDatabaseName.equals(catalogSinkTableIde.getDatabaseName) &&
        createTableIde.getObjectName.equals(catalogSinkTableIde.getObjectName) &&
        createTableIde.getCatalogName.equals(catalogSinkTableIde.getCatalogName)) {
        return Some(createTableOps)
      }
    }
    None
  }
}
