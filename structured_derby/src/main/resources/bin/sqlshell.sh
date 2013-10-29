#!/bin/bash

# Splice Machine SQL Shell
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

CLASSPATH=""

MYCLASSPATH="${DIR}"/lib/*

GEN_SYS_ARGS="-Djava.awt.headless=true -Dlog4j.configuration=file:${DIR}/lib/info-log4j.properties"

IJ_SYS_ARGS="-Djdbc.drivers=org.apache.derby.jdbc.ClientDriver -Dij.connection.splice=jdbc:derby://localhost:1527/splicedb"

echo "Running Splice Machine SQL shell"
echo "For help: \"splice> help;\""
java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS} -classpath "${MYCLASSPATH}"  org.apache.derby.tools.ij $*
