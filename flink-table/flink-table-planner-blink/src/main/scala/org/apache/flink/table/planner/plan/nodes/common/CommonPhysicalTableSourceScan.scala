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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.connector.source.{InputFormatProvider, ScanTableSource, SourceFunctionProvider, SourceFunctionProviderWithExternalConfig, SourceFunctionProviderWithParallel}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.api.common.NodeShardInfo
import org.apache.flink.table.slotgroup.SlotGroupOptions

import scala.collection.JavaConverters._

/**
  * Base physical RelNode to read data from an external source defined by a [[ScanTableSource]].
  */
abstract class CommonPhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: TableSourceTable)
  extends TableScan(cluster, traitSet, relOptTable) {

  // cache table source transformation.
  protected var sourceTransform: Transformation[_] = _

  protected val tableSourceTable: TableSourceTable = relOptTable.unwrap(classOf[TableSourceTable])

  protected[flink] val tableSource: ScanTableSource =
    tableSourceTable.tableSource.asInstanceOf[ScanTableSource]

  override def deriveRowType(): RelDataType = {
    // TableScan row type should always keep same with its
    // interval RelOptTable's row type.
    relOptTable.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
  }

  protected def createSourceTransformation(
      env: StreamExecutionEnvironment,
      name: String,
      operatorType: String): Transformation[RowData] = {
    val runtimeProvider = tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE)
    val outRowType = FlinkTypeFactory.toLogicalRowType(tableSourceTable.getRowType)
    val outTypeInfo = new RowDataTypeInfo(outRowType)

    val transformation = {
    runtimeProvider match {
      case provider: SourceFunctionProviderWithExternalConfig =>
        val sourceFunction = provider.createSourceFunction()
        if (provider.getConfiguredParallel > 0) {
          env
            .addSource(sourceFunction, name, operatorType,outTypeInfo)
            .setParallelism(provider.getConfiguredParallel)
            .getTransformation
        }else{
          // when parallel is not set, compare the tp size
          val customSourceTransform = env
            .addSource(sourceFunction, name, operatorType,outTypeInfo)
            .getTransformation
          env.getConfig.getLeafNodeShardState.put(
            customSourceTransform.getId,
            new NodeShardInfo(true, provider.getShardNumber)
          )

          customSourceTransform
        }
      // originally not support for the parallel setting
      // add the parallel setting in the source function
      case provider: SourceFunctionProviderWithParallel =>
        val sourceFunction = provider.createSourceFunction()
        if (provider.getConfiguredParallel > 0) {
          env
            .addSource(sourceFunction, name, operatorType, outTypeInfo)
            .setParallelism(provider.getConfiguredParallel)
            .getTransformation
        }else{
          // when parallel is not set, compare the tp size
          val customSourceTransform = env
            .addSource(sourceFunction, name, operatorType, outTypeInfo)
            .getTransformation
          customSourceTransform
        }
      case provider: SourceFunctionProvider =>
        val sourceFunction = provider.createSourceFunction()
        env
          .addSource(sourceFunction, name, operatorType, outTypeInfo)
          .getTransformation
      case provider: InputFormatProvider =>
        val inputFormat = provider.createInputFormat()
        createInputFormatTransformation(env, inputFormat, name, operatorType, outTypeInfo)
    }
    }

    val slotGroup = tableSourceTable.catalogTable.getOptions.get(SlotGroupOptions.SLOT_SLOT.key())
    if(slotGroup != null){
      transformation.setSlotSharingGroup(slotGroup)
    }

    transformation
  }

  /**
   * Creates a [[Transformation]] based on the given [[InputFormat]].
   * The implementation is different for streaming mode and batch mode.
   */
  protected def createInputFormatTransformation(
      env: StreamExecutionEnvironment,
      inputFormat: InputFormat[RowData, _],
      name: String,
      operatorType: String,
      outTypeInfo: RowDataTypeInfo): Transformation[RowData]
}
