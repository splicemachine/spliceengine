#!/bin/bash

test -L "${BASH_SOURCE[0]}"
if [[ $? -eq 0 ]] ; then
    if [[ `uname` == CYGWIN* ]]; then
        ME=$(readlink $(cygpath "${BASH_SOURCE[0]}"))
    else
        ME=$(readlink "${BASH_SOURCE[0]}")
    fi
else
    ME="${BASH_SOURCE[0]}"
fi

ROOT_DIR="$( cd "$( dirname "${ME}" )/.." && pwd )"
source ${ROOT_DIR}/bin/functions.sh

# Splice Machine SQL Shell

if [[ -n "${LOG4J_PROP_PATH}" ]]; then
    # Allow users to set their own log file if debug required
    LOG4J_PATH="${1}"
else
    LOG4J_PATH="file:${ROOT_DIR}/lib/info-log4j.properties"
fi

# wjk
DERBY_CP="/Users/wkoetke/Documents/workspace-k2/derby"
DEV_CP="/Users/wkoetke/Documents/workspace-k2/spliceengine"

CLASSPATH="${DERBY_CP}/engine_it/target/engine_it-2.0.0-SNAPSHOT.jar:\
${DERBY_CP}/db-build/target/db-build-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-client/target/db-client-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-client/target/original-db-client-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-drda/target/db-drda-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-engine/target/db-engine-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-shared/target/db-shared-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-testing/target/db-testing-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-tools-i18n/target/db-tools-i18n-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-tools-ij/target/db-tools-ij-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DERBY_CP}/db-tools-testing/target/db-tools-testing-2.0.0.k2_refactor-SNAPSHOT.jar:\
${DEV_CP}/hbase_ddl/target/hbase_ddl-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/hbase_pipeline/target/hbase_pipeline-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/hbase_sql/target/hbase_sql-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/hbase_storage/target/hbase_storage-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/mem_engine/target/mem_engine-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/mem_pipeline/target/mem_pipeline-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/mem_storage/target/mem_storage-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/pipeline_api/target/pipeline_api-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/splice_access_api/target/splice_access_api-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/splice_encoding/target/splice_encoding-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/splice_machine/target/splice_machine-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/splice_protocol/target/splice_protocol-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/splice_si_api/target/splice_si_api-2.0.0-SNAPSHOT.jar:\
${DEV_CP}/splice_timestamp_api/target/splice_timestamp_api-2.0.0-SNAPSHOT.jar"

# set up isolated classpath.
# If not in dev env, DEV_CP will be empty
CLASSPATH="${CLASSPATH}:${DEV_CP}:${ROOT_DIR}/lib/*"

ECHO $CLASSPATH

if [[ ${UNAME} == CYGWIN* ]]; then
    CLASSPATH=$(cygpath --path --windows "${ROOT_DIR}/lib/*")
    LOG4J_PATH="file:///$(cygpath --path --windows ${ROOT_DIR}/lib/info-log4j.properties)"
fi
export CLASSPATH

LOG4J_CONFIG="-Dlog4j.configuration=${LOG4J_PATH}"

GEN_SYS_ARGS="-Djava.awt.headless=true ${LOG4J_CONFIG}"

IJ_SYS_ARGS="-cp $CLASSPATH -Djdbc.drivers=com.splicemachine.db.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"

if hash rlwrap 2>/dev/null; then
    echo -en "\n ========= rlwrap detected and enabled.  Use up and down arrow keys to scroll through command line history. ======== \n\n"
    RLWRAP=rlwrap
else
    echo -en "\n ========= rlwrap not detected.  Consider installing for command line history capabilities. ========= \n\n"
    RLWRAP=
fi

echo "Running Splice Machine SQL shell"
echo "For help: \"splice> help;\""
${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS}  com.splicemachine.db.tools.ij $*
