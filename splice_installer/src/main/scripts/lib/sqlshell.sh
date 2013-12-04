#!/bin/bash

# Splice Machine SQL Shell
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

CLASSPATH=""

# sets VM specific env vars
. "${ROOT_DIR}"/setEnv

echo "Running Splice Machine SQL shell"

CLASSPATH=${HBASE_HOME}/hbase.jar:${HBASE_LIB}/*
export CLASSPATH

GEN_SYS_ARGS="-Djava.awt.headless=true"

IJ_SYS_ARGS="-Djdbc.drivers=org.apache.derby.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://localhost:1527/splicedb"

if hash rlwrap 2>/dev/null; then
    echo " ========= rlwrap detected and enabled.  Use up and down arrow keys to scroll through command line history. ======== "
    echo
    RLWRAP=rlwrap
else
    echo " ========= rlwrap not detected.  Consider installing for command line history capabilities. ========= "
    echo
    RLWRAP=
fi

echo "Running Splice Machine SQL shell"
echo "For help: \"splice> help;\""
${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS}  org.apache.derby.tools.ij $*
