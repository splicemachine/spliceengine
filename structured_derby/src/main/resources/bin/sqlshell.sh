#!/bin/bash

# Splice Machine SQL Shell
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

LOG4J_PATH="file:${ROOT_DIR}/lib/info-log4j.properties"
CLASSPATH="${ROOT_DIR}/lib/*"

CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    CLASSPATH=`cygpath --path --windows "${ROOT_DIR}/lib/*"`
    LOG4J_PATH="file:///`cygpath --path --windows ${ROOT_DIR}/lib/info-log4j.properties`"
fi
export CLASSPATH
LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_PATH"

GEN_SYS_ARGS="-Djava.awt.headless=true ${LOG4J_CONFIG}"

IJ_SYS_ARGS="-Djdbc.drivers=org.apache.derby.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://localhost:1527/splicedb"

echo "Running Splice Machine SQL shell"
echo "For help: \"splice> help;\""
java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS}  org.apache.derby.tools.ij $*
