#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${ROOT_DIR}/bin/functions.sh

# Splice Machine SQL Shell

if [[ -n "${LOG4J_PROP_PATH}" ]]; then
    # Allow users to set their own log file if debug required
    LOG4J_PATH="${1}"
else
    LOG4J_PATH="file:${ROOT_DIR}/lib/info-log4j.properties"
fi

# set up isolated classpath.
# If not in dev env, DEV_CP will be empty
CLASSPATH="${DEV_CP}:${ROOT_DIR}/lib/*"

if [[ ${UNAME} == CYGWIN* ]]; then
    CLASSPATH=$(cygpath --path --windows "${ROOT_DIR}/lib/*")
    LOG4J_PATH="file:///$(cygpath --path --windows ${ROOT_DIR}/lib/info-log4j.properties)"
fi
export CLASSPATH

LOG4J_CONFIG="-Dlog4j.configuration=${LOG4J_PATH}"

GEN_SYS_ARGS="-Djava.awt.headless=true ${LOG4J_CONFIG}"

IJ_SYS_ARGS="-Djdbc.drivers=org.apache.derby.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://localhost:1527/splicedb"

if hash rlwrap 2>/dev/null; then
    echo -en "\n ========= rlwrap detected and enabled.  Use up and down arrow keys to scroll through command line history. ======== \n\n"
    RLWRAP=rlwrap
else
    echo -en "\n ========= rlwrap not detected.  Consider installing for command line history capabilities. ========= \n\n"
    RLWRAP=
fi

echo "Running Splice Machine SQL shell"
echo "For help: \"splice> help;\""
${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS}  org.apache.derby.tools.ij $*
