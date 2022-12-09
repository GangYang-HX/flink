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

import java.util

import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api.PhysicalExecutionPlan
import org.apache.flink.table.api.PhysicalExecutionPlan.PhysicalExecutionPlanBuilder

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object PlanUtil extends Logging {

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
}
