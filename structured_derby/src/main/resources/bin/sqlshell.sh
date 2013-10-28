#!/bin/bash

# Splice Machine SQL Shell
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

CLASSPATH=""

MYCLASSPATH="${DIR}"/lib/*

GEN_SYS_ARGS="-Djava.awt.headless=true -Dlog4j.configuration=file:${DIR}/lib/info-log4j.properties"


echo "Running Splice Machine SQL shell"

echo "Connect using: [connect 'jdbc:derby://localhost:1527/splicedb';]"
java ${GEN_SYS_ARGS} -classpath "$MYCLASSPATH" -Djdbc.drivers=org.apache.derby.jdbc.ClientDriver org.apache.derby.tools.ij $*
