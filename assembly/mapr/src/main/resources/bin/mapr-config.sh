#
#/**
# * Copyright The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Source env.sh from MapR distribution
BASE_MAPR=${MAPR_HOME:-/opt/mapr}
env=${BASE_MAPR}/conf/env.sh
[ -f $env ] && . $env

# Set the user if not set in the environment
if [ "$HBASE_IDENT_STRING" == "" ]; then
  HBASE_IDENT_STRING=`id -nu`
fi

# Dump heap on OOM
HBASE_OPTS="$HBASE_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/opt/cores/"

# Add MapR file system and dependency jars. There are two sets of such jars
# First set which override those found in HBase' lib folder, and is prepended
# to the CLASSPATH while the second set is appended to HBase' classpath.

# First set 
# JARs in ${BASE_MAPR}/lib
MAPR_JARS="zookeeper-3.4*.jar"
for jar in ${MAPR_JARS}; do
  JARS=`echo $(ls ${BASE_MAPR}/lib/${jar} 2> /dev/null) | sed 's/\s\+/:/g'`
  if [ "${JARS}" != "" ]; then
    HBASE_MAPR_OVERRIDE_JARS=${HBASE_MAPR_OVERRIDE_JARS}:${JARS}
  fi
done
# Remove any additional ':' from the tail
HBASE_MAPR_OVERRIDE_JARS="${HBASE_MAPR_OVERRIDE_JARS#:}"

# Second set
# JARs in ${BASE_MAPR}/lib
MAPR_JARS="libprotodefs*.jar baseutils*.jar JPam-*.jar"
for jar in ${MAPR_JARS}; do
  JARS=`echo $(ls ${BASE_MAPR}/lib/${jar} 2> /dev/null) | sed 's/\s\+/:/g'`
  if [ "${JARS}" != "" ]; then
    HBASE_MAPR_EXTRA_JARS=${HBASE_MAPR_EXTRA_JARS}:${JARS}
  fi
done
# Remove any additional ':' from the tail
HBASE_MAPR_EXTRA_JARS="${HBASE_MAPR_EXTRA_JARS#:}"

export HBASE_OPTS HBASE_MAPR_OVERRIDE_JARS HBASE_MAPR_EXTRA_JARS HBASE_IDENT_STRING
