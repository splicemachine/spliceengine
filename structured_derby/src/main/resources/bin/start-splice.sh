#!/bin/bash

# Start with debug logging by passing this script the "-debug" argument

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
LOGFILE="${ROOT_DIR}"/splice.log
DEBUG=$1

# server still running? - must stop first
if [[ -e "${ROOT_DIR}"/splice_pid || -e "${ROOT_DIR}"/zoo_pid ]]; then
    echo "Splice still running and must be shut down. Run stop-splice.sh"
    exit 1;
fi

# Must have Java installed
if [[ -z $(type -p java) ]]; then
    echo checking for Java...
    if [[ -z "$JAVA_HOME" ]] || [[ ! -x "$JAVA_HOME/bin/java" ]];  then
        echo Must have Java installed
        exit 1;
    fi
fi

# Config server
CP="${ROOT_DIR}/lib/*"
ZOO_DIR="${ROOT_DIR}"/db/zookeeper
HBASE_ROOT_DIR_URI="file://${ROOT_DIR}/db/hbase"

LOG4J_PATH="file:${ROOT_DIR}/lib/info-log4j.properties"
if [[ -n "$DEBUG" && "$DEBUG" -eq "-debug" ]]; then
    LOG4J_PATH="file:${ROOT_DIR}/lib/hbase-log4j.properties"
fi

# Config for Cygwin, if necessary
CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    # cygwin likes to write in 3 places for /tmp
    # we'll symlink them
    if [[ -e "/tmp" && ! -L "/tmp" ]]; then
        rm -rf "/tmp_bak"
        mv "/tmp" "/tmp_bak"
    fi
    if [[ ! -e "/tmp" && ! -L "/tmp" ]]; then
        ln -s "/cygdrive/c/tmp" "/tmp"
    fi
    if [[ ! -e "/temp" && ! -L "/temp" ]]; then
        ln -s "/cygdrive/c/tmp" "/temp"
    fi

    # cygwin paths look a little different
    CP=`cygpath --path --windows "${ROOT_DIR}/lib/*"`
    ZOO_DIR=`cygpath --path --windows "${ROOT_DIR}/db/zookeeper"`
    HBASE_ROOT_DIR_URI="CYGWIN"
    LOG4J_PATH="file:///`cygpath --path --windows ${ROOT_DIR}/lib/info-log4j.properties`"
    if [[ -n "$DEBUG" && "$DEBUG" -eq "-debug" ]]; then
        LOG4J_PATH="file:///`cygpath --path --windows ${ROOT_DIR}/lib/hbase-log4j.properties`"
    fi
fi

# Start server with retry logic
ZOO_WAIT_TIME=60
SPLICE_MAIN_CLASS="com.splicemachine.single.SpliceSinglePlatform"
"${ROOT_DIR}"/bin/_retrySplice.sh "${ROOT_DIR}" "${LOGFILE}" "${LOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" ${ZOO_WAIT_TIME} "${HBASE_ROOT_DIR_URI}" "${CP}" ${SPLICE_MAIN_CLASS} "FALSE"
