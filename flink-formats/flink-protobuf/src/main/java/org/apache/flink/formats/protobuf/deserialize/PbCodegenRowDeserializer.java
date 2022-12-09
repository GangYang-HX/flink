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

package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.formats.protobuf.PbCodegenAppender;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbCodegenVarId;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

/** Deserializer to convert proto message type object to flink row type data. */
public class PbCodegenRowDeserializer implements PbCodegenDeserializer {
    private final Descriptor descriptor;
    private final RowType rowType;
    private final PbFormatConfig formatConfig;

    public PbCodegenRowDeserializer(
            Descriptor descriptor, RowType rowType, PbFormatConfig formatConfig) {
        this.rowType = rowType;
        this.descriptor = descriptor;
        this.formatConfig = formatConfig;
    }

    @Override
    public String codegen(String returnInternalDataVarName, String pbGetStr)
            throws PbCodegenException {
        // The type of messageGetStr is a native pb object,
        // it should be converted to RowData of flink internal type
        PbCodegenAppender appender = new PbCodegenAppender();
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        String pbMessageVar = "message" + uid;
        String rowDataVar = "rowData" + uid;

        int fieldSize = rowType.getFieldNames().size();
        String pbMessageTypeStr = PbFormatUtils.getFullJavaName(descriptor);
        appender.appendLine(pbMessageTypeStr + " " + pbMessageVar + " = " + pbGetStr);
        appender.appendLine(
                "GenericRowData " + rowDataVar + " = new GenericRowData(" + fieldSize + ")");
        String fieldNamesVar = "fieldNames" + uid;
        String methodString =
                "Method[] "
                        + fieldNamesVar
                        + " ="
                        + pbMessageVar
                        + ".getClass().getDeclaredMethods()";

        appender.appendLine(methodString);
        String fieldNameListVar = "fieldNameList" + uid;
        appender.appendLine("List<String> " + fieldNameListVar + "= new ArrayList()");
        appender.appendSegment(
                "for (Method name : "
                        + fieldNamesVar
                        + ") {"
                        + "            "
                        + fieldNameListVar
                        + ".add(name.getName());"
                        + "        }");
        int index = 0;
        for (String fieldName : rowType.getFieldNames()) {
            int subUid = varUid.getAndIncrement();
            String elementDataVar = "elementDataVar" + subUid;

            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            FieldDescriptor elementFd = descriptor.findFieldByName(fieldName);
            String strongCamelFieldName = PbFormatUtils.getStrongCamelCaseJsonName(fieldName);
            PbCodegenDeserializer codegen =
                    PbCodegenDeserializeFactory.getPbCodegenDes(elementFd, subType, formatConfig);
            appender.appendLine("Object " + elementDataVar + " = null");
            if (!formatConfig.isReadDefaultValues()) {
                // only works in syntax=proto2 and readDefaultValues=false
                // readDefaultValues must be true in pb3 mode
                String isMessageNonEmptyStr =
                        isMessageNonEmptyStr(
                                pbMessageVar,
                                strongCamelFieldName,
                                PbFormatUtils.isRepeatedType(subType));
                appender.appendSegment("if(" + isMessageNonEmptyStr + "){");
            }
            String elementMessageGetStr =
                    pbMessageElementGetStr(
                            pbMessageVar,
                            strongCamelFieldName,
                            elementFd,
                            PbFormatUtils.isArrayType(subType));
            if (ignoreNullRows(formatConfig, elementFd)) {
                String sb =
                        "if(!"
                                + pbMessageVar
                                + ".has"
                                + strongCamelFieldName
                                + "()"
                                + "){"
                                + rowDataVar
                                + ".setField("
                                + index
                                + ","
                                + "null"
                                + ");"
                                + "}else{";
                appender.appendSegment(sb);
                String code = codegen.codegen(elementDataVar, elementMessageGetStr);
                appender.appendSegment(code);
                if (!formatConfig.isReadDefaultValues()) {
                    appender.appendSegment("}");
                }
                appender.appendLine(
                        rowDataVar + ".setField(" + index + ", " + elementDataVar + ")");
                appender.appendLine("}");
            } else {
                /*    exp$:
                 *
                 *    boolean flag1 = false;
                 *    if(!fieldNameList0.contains("hasName")){
                 *        flag1= false;
                 *    }else{
                 *        try{ flag1 = !(Boolean)message0.getClass().getDeclaredMethod("hasName").invoke(message0);
                 *           } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                 *
                 *           };
                 *    };
                 *    if(flag1){
                 *        rowData0.setField(0, null);
                 *    } else {
                 *         elementDataVar1 = BinaryStringData.fromString(message0.getName().toString());
                 *         rowData0.setField(0, elementDataVar1);
                 *    };
                 *
                 *
                 */
                appender.appendLine("boolean flag" + subUid + " = false");
                appender.appendSegment(
                        "if(!"
                                + fieldNameListVar
                                + ".contains(\"has"
                                + strongCamelFieldName
                                + "\")){");
                appender.appendLine("flag" + subUid + "= false");
                appender.appendSegment("} else {");
                String invokeString =
                        "try{ flag"
                                + subUid
                                + " = !(Boolean)"
                                + pbMessageVar
                                + ".getClass().getDeclaredMethod(\"has"
                                + strongCamelFieldName
                                + "\").invoke("
                                + pbMessageVar
                                + ")";
                appender.appendLine(invokeString);
                appender.appendSegment(
                        "} catch (NoSuchMethodException noSuchMethodException"
                                + subUid
                                + "){} catch (IllegalAccessException illegalAccessException"
                                + subUid
                                + "){} catch (InvocationTargetException invocationTargetException"
                                + subUid
                                + "){}");
                appender.appendSegment("}");
                String oneOfString = "if(flag" + subUid + "){";
                appender.appendSegment(oneOfString);
                appender.appendLine(rowDataVar + ".setField(" + index + ", null" + ")");
                appender.appendSegment("}else{");
                String code = codegen.codegen(elementDataVar, elementMessageGetStr);
                appender.appendSegment(code);
                if (!formatConfig.isReadDefaultValues()) {
                    appender.appendSegment("}");
                }
                appender.appendLine(
                        rowDataVar + ".setField(" + index + ", " + elementDataVar + ")");
                appender.appendLine("}");
            }
            index += 1;
        }
        appender.appendLine(returnInternalDataVarName + " = " + rowDataVar);
        return appender.code();
    }

    private boolean ignoreNullRows(PbFormatConfig formatConfig, FieldDescriptor elementFd) {
        return !formatConfig.isIgnoreNullRows()
                && FieldDescriptor.Type.MESSAGE.equals(elementFd.getType())
                && !elementFd.isRepeated();
    }

    private String pbMessageElementGetStr(
            String message, String fieldName, FieldDescriptor fd, boolean isList) {
        if (fd.isMapField()) {
            // map
            return message + ".get" + fieldName + "Map()";
        } else if (isList) {
            // list
            return message + ".get" + fieldName + "List()";
        } else {
            return message + ".get" + fieldName + "()";
        }
    }

    private String isMessageNonEmptyStr(String message, String fieldName, boolean isListOrMap) {
        if (isListOrMap) {
            return message + ".get" + fieldName + "Count() > 0";
        } else {
            // proto syntax class do not have hasName() interface
            return message + ".has" + fieldName + "()";
        }
    }
}
