#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

##
## Variables with defaults (if not overwritten by environment)
##
SCALA_VERSION=2.12
SKIP_GPG=true
RELEASE_VERSION=1.15.1-SNAPSHOT
MVN=mvn

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

cd ..

FLINK_DIR=`pwd`
RELEASE_DIR=${FLINK_DIR}/tools/releasing/release
PYTHON_RELEASE_DIR=${RELEASE_DIR}/python
mkdir -p ${PYTHON_RELEASE_DIR}

###########################

# build maven package, create Flink distribution, generate signature
make_binary_release() {
  FLAGS=""
  SCALA_VERSION=$1

  echo "Creating binary release, SCALA_VERSION: ${SCALA_VERSION}"
  dir_name="flink-$RELEASE_VERSION-bin-scala_${SCALA_VERSION}"

  if [ $SCALA_VERSION = "2.12" ]; then
      FLAGS="-Dscala-2.12"
  else
      echo "Invalid Scala version ${SCALA_VERSION}"
  fi

  # enable release profile here (to check for the maven version)
  $MVN clean package $FLAGS -pl flink-dist -am -Dgpg.skip -Dcheckstyle.skip=true -DskipTests -T 8

  cd flink-dist/target/flink-${RELEASE_VERSION}-bin
  ${FLINK_DIR}/tools/releasing/collect_license_files.sh ./flink-${RELEASE_VERSION} ./flink-${RELEASE_VERSION}
  tar czf "${dir_name}.tgz" flink-*

  cp flink-*.tgz ${RELEASE_DIR}
  cd ${RELEASE_DIR}

  # Sign sha the tgz
  if [ "$SKIP_GPG" == "false" ] ; then
    gpg --armor --detach-sig "${dir_name}.tgz"
  fi
  $SHASUM "${dir_name}.tgz" > "${dir_name}.tgz.sha512"

  cd ${FLINK_DIR}
}

if [ "$SCALA_VERSION" == "none" ]; then
  make_binary_release "2.12"
else
  make_binary_release "$SCALA_VERSION"
fi
