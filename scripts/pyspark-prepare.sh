#!/usr/bin/env bash
#
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# Figure out where Gnocchi is installed
SCRIPT_DIR="$(cd `dirname $0`/..; pwd)"

# Find Gnocchi cli assembly jar
GNOCCHI_CLI_JAR=
if [ -d "$SCRIPT_DIR/repo" ]; then
  ASSEMBLY_DIR="$SCRIPT_DIR/repo"
else
  ASSEMBLY_DIR="$SCRIPT_DIR/gnocchi-assembly/target"
fi

num_jars="$(ls -1 "$ASSEMBLY_DIR" | grep "^gnocchi[0-9A-Za-z\.\_\-]*\.jar$" | grep -v javadoc | grep -v sources | wc -l)"
if [ "$num_jars" -eq "0" ]; then
  echo "Failed to find Gnocchi cli assembly in $ASSEMBLY_DIR." 1>&2
  echo "You need to build Gnocchi before running this program." 1>&2
  exit 1
fi

ASSEMBLY_JARS="$(ls -1 "$ASSEMBLY_DIR" | grep "^gnocchi[0-9A-Za-z\.\_\-]*\.jar$" | grep -v javadoc | grep -v sources || true)"
if [ "$num_jars" -gt "1" ]; then
  echo "Found multiple Gnocchi cli assembly jars in $ASSEMBLY_DIR:" 1>&2
  echo "$ASSEMBLY_JARS" 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi

GNOCCHI_CLI_JAR="${ASSEMBLY_DIR}/${ASSEMBLY_JARS}"

export PYSPARK_SUBMIT_ARGS="--jars ${GNOCCHI_CLI_JAR} --driver-class-path ${GNOCCHI_CLI_JAR} pyspark-shell"
