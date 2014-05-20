#!/bin/bash

# Splice Machine SQL Shell
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"

LOG4J_PROP_PATH="file:${ROOT_DIR}/target/splicemachine/lib/info-log4j.properties"
export LOG4J_PROP_PATH
# DEV_CP will be overridden in stand-alone script
DEV_CP="${ROOT_DIR}/target/splicemachine/lib/*"
export DEV_CP

SQL_SHELL="${ROOT_DIR}"/target/classes/bin/sqlshell.sh

if [[ ! -x ${SQL_SHELL} ]]; then
    chmod 755 ${SQL_SHELL}
fi

${SQL_SHELL} $*
