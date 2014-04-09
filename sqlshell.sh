#!/bin/bash

chmod 777 structured_derby/target/classes/bin/sqlshell.sh

# Splice Machine SQL Shell
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"

LOG4J_PROP_PATH="file:${ROOT_DIR}/target/splicemachine/lib/info-log4j.properties"
export LOG4J_PROP_PATH
CLASSPATH="${ROOT_DIR}/target/splicemachine/lib/*"
export CLASSPATH

"${ROOT_DIR}"/target/splicemachine/bin/sqlshell.sh $*
