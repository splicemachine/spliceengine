#!/bin/bash

SPID=$(ps -ef | awk '/SpliceTestPlatform|SpliceSinglePlatform|SpliceTestClusterParticipant/ && !/awk/ {print $2}')
ZPID=$(ps -ef | awk '/zookeeper/ && !/awk/ {print $2}')
YPID=$(ps -ef | awk '/spliceYarn|SpliceTestYarnPlatform|CoarseGrainedScheduler|ExecutorLauncher/ && !/awk/ {print $2}')
KPID=$(ps -ef | awk '/TestKafkaCluster/ && !/awk/ {print $2}')

if [[ -z ${SPID} || -z ${ZPID} ]] || [[ -z ${YPID} ]]; then
     echo "Splice Machine is not running.  Make sure the database is started."
     exit 1;
fi
if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|                    Error: JAVA_HOME is not set                       |
+----------------------------------------------------------------------+
|                Please download and configure Java                    |
|              sqlshell requires Java 1.8 or openjdk8                  |
+======================================================================
EOF
    exit 1
fi


BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${BASE_DIR}/bin/functions.sh

LOG4J_FILE="file://${BASE_DIR}/conf/info-log4j.properties"

CLASSPATH="${BASE_DIR}/lib/*"
export CLASSPATH

LOG4J_CONFIG="-Dlog4j.configuration=${LOG4J_FILE}"

GEN_SYS_ARGS="-Djava.awt.headless=true ${LOG4J_CONFIG}"

IJ_SYS_ARGS="-Djdbc.drivers=com.splicemachine.db.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"

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
