#!/bin/bash

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${BASE_DIR}/bin/functions.sh

DB_DIR="${BASE_DIR}"/db

# Clean the Splice Machine database

# if server still running, fail - must stop first
SPID=$(ps -ef | awk '/SpliceTestPlatform|SpliceSinglePlatform|SpliceTestClusterParticipant/ && !/awk/ {print $2}')
ZPID=$(ps -ef | awk '/zookeeper/ && !/awk/ {print $2}')
YPID=$(ps -ef | awk '/spliceYarn|SpliceTestYarnPlatform|CoarseGrainedScheduler|ExecutorLauncher/ && !/awk/ {print $2}')
KPID=$(ps -ef | awk '/TestKafkaCluster/ && !/awk/ {print $2}')
if [[ -n ${SPID} || -n ${ZPID} ]] || [[ -n ${YPID} || -n ${KPID} ]]; then
     echo "Splice still running and must be shut down. Run stop-splice.sh"
     exit 1;
fi

echo "Cleaning the Splice Machine database..."

/bin/rm -rf "${BASE_DIR}"/log/target/SpliceTestYarnPlatform
/bin/rm -rf "${DB_DIR}"/zookeeper
/bin/rm -rf "${DB_DIR}"/hbase
# TODO do I have to do this?
# /bin/rm -rf "${BASE_DIR}"/lib/yarn-site.xml

echo "done."
