#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Start a Flink JobManager for native Kubernetes.
# NOTE: This script is not meant to be started manually. It will be used by native Kubernetes integration.

(>&1 echo "[INFO] Starting fecher.")

USAGE="Usage: kubernetes-jars-fetcher.sh [args]"
export FLINK_FETCHER_HOME=${FLINK_FETCHER_HOME}
export JVM_ARGS=${JVM_ARGS}
CLASS_TO_RUN=org.apache.flink.kubernetes.fetcher.HadoopFetcher
JAVA_RUN=java

if [ "$FLINK_FETCHER_HOME" = "" ]; then
    echo "[ERROR] Please specify FLINK_FETCHER_HOME." 1>&2
    exit 1
fi

# shellcheck disable=SC2006
JAR_FILES="`find "$FLINK_FETCHER_HOME" -name '*.jar' -print | tr "\n" ":"`"
FLINK_FETCHER_CLASSPATH="$JAR_FILES";

if [ "$FLINK_FETCHER_CLASSPATH" = "" ]; then
    echo "[ERROR] Fetcher jar not found in $FLINK_FETCHER_HOME." 1>&2
    exit 1
fi

ARGS=("${@:1}")
LOG_FILE=${FLINK_FETCHER_HOME}/logs/fetcher.log
LOG_CONF_FILES="`find "$FLINK_FETCHER_HOME" -name 'log4j-fetcher.properties'`"
JVM_ARGS+=" -Dlog.file=${LOG_FILE} -Dlog4j2.configurationFile=file://${LOG_CONF_FILES}"

(>&1 echo "[INFO] FLINK_FETCHER_CLASSPATH: ${FLINK_FETCHER_CLASSPATH}")
(>&1 echo "[INFO] ARGS:" "${ARGS[@]}")
(>&1 echo "[INFO] JVM_ARGS:" "${JVM_ARGS}")

"$JAVA_RUN" $JVM_ARGS -classpath "${FLINK_FETCHER_CLASSPATH}"  ${CLASS_TO_RUN} "${ARGS[@]}"
