#!/bin/bash

##################################################################################
# Start Zookeeper and the Splice HBase master server and region servers and yarn
#
# Currently, if compilation fails, it still tries to start Zoo and RSes.
#   An enhancement would be to tee build output to a Named Pipe to monitor for
#   "[INFO] BUILD SUCCESS" before starting, like this example:
#   mkfifo -m 770 /tmp/myfifo
#   iter=0 ; while true ; do echo $iter 2>&1 | tee /tmp/myfifo ; iter=$((${iter}+1)) ; sleep 1 ; done
#   while true ; do sleep 1 ; grep -q 6 /tmp/myfifo && echo 'i found a 6!' ; done
##################################################################################

##################################################################################
# Function to kill all splice test processes - zoo, SpliceTestPlatform, YARN, and
# anything started from maven exec, i.e., mvn exec:exec
##################################################################################

# if server still running, fail - must stop first
SPID=$(ps -ef | awk '/SpliceTestPlatform|SpliceSinglePlatform|SpliceTestClusterParticipant|OlapServerMaster/ && !/awk/ {print $2}')
ZPID=$(ps -ef | awk '/ZooKeeper/ && !/awk/ {print $2}')
YPID=$(ps -ef | awk '/spliceYarn|SpliceTestYarnPlatform|CoarseGrainedScheduler|ExecutorLauncher/ && !/awk/ {print $2}')
KPID=$(ps -ef | awk '/TestKafkaCluster/ && !/awk/ {print $2}')
if [[ -n ${SPID} || -n ${ZPID} ]] || [[ -n ${YPID} || -n ${KPID} ]]; then
     echo "Splice Machine is already running.  No action taken."
     exit 1;
fi


echo "Starting the Splice Machine database..."
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${BASE_DIR}/bin/functions.sh


DEFAULT_PROFILE="cdh5.8.3"  # default hbase platform profile
PROFILE=$DEFAULT_PROFILE
IN_MEM_PROFILE="mem"
RUN_DIR="${BASE_DIR}/log"

if [ ! -d $RUN_DIR ]; then
    mkdir -p $RUN_DIR
fi

if [ ! -d $BASE_DIR/db ]; then
    mkdir -p $BASE_DIR/db
fi

chmod -R 777 $BASE_DIR/demodata

# never building
BUILD="0"
CHAOS="false"

MEMBERS=2
DEBUG_PATH=""

# no mvn
MVN='mvn -B'

# TODO review all of these
usage() {
    # $1 is an error, if any
    if [[ -n "${1}" ]]; then
        echo "Error: ${1}"
    fi
    echo "Usage: $0 -p [<hbase_profile>] [-s <n>] [-c] [-k] -h[elp]"
    echo "Where: "
    echo "  -k just KILL any and all splice processes currently running."
    echo "  -h => print this message"
}

while getopts "chkp:s:bd:" flag ; do
    case $flag in
        h* | \?)
            usage
            exit 0 # This is not an error, User asked help. Don't do "exit 1"
        ;;
        d)
        # path to write debug file
           DEBUG_PATH=$OPTARG
        ;;
        k)
        # KILL current processes
           _kill_em_all 9
           exit 0
        ;;
        ?)
            usage "Unknown option (ignored): ${OPTARG}"
            exit 1
        ;;
    esac
done

SYSTEM_PROPS="-Dlog4j.configuration=hbase-log4j.properties -DfailTasksRandomly=${CHAOS}"

#=============================================================================
# Run...
#=============================================================================
_kill_em_all 9


# echo "Running Splice $PROFILE master and ${MEMBERS} regionservers with CHAOS = ${CHAOS} in:"
# echo "   ${RUN_DIR}"

pushd "${RUN_DIR}" > /dev/null
## delete old logs before we start fresh
/bin/rm -f *.log*

ZOO_LOG="${RUN_DIR}/zoo.log"
echo "Starting ZooKeeper. Log file is ${ZOO_LOG}"


# TODO eventually move - need cp script to consolidate as part of prepare package
CP="${BASE_DIR}/lib":"${BASE_DIR}/lib/*:${BASE_DIR}/conf"
LOG4J_FILE="file://${BASE_DIR}/conf/info-log4j.properties"

ZOO_DIR="${BASE_DIR}"/db/zookeeper

_startZoo "${BASE_DIR}" "${ZOO_LOG}" "${LOG4J_FILE}" "${ZOO_DIR}" "${CP}"

#######################################################################################################
# Wait for up to 65 seconds for zoo to start, checking nc periodically to see if we are connected
# This makes use of ZooKeeper's 'Four-Letter Words' commands to determine the liveness of ZooKeeper
# In particular, it uses netcat (nc) to pipe the command 'ruok' to zookeeper on it's connection port.
# If ZooKeeper responds with 'imok', then the server is up. If it returns empty, then zookeeper
# hasn't started yet. If the count goes to 0, and we never got 'imok', then we want to blow up because
# ZooKeeper didn't start up properly
#######################################################################################################
COUNT=65
ZOO_UP=""
until [ ${COUNT} -eq 0 ] || [ "${ZOO_UP}" == "imok" ]; do
    sleep 1
    ZOO_UP="$(echo 'ruok' | nc localhost 2181)"
    let COUNT-=1
done

if [ ${COUNT} -eq 0 ]; then
    echo "ZooKeeper did not start up properly, aborting startup. Please check ${ZOO_LOG} for more information"
    exit 5
fi


YARN_LOG="${RUN_DIR}/yarn.log"
echo "Starting YARN. Log file is ${YARN_LOG}"

_startYarn "${BASE_DIR}" "${CP}" "${YARN_LOG}"


KAFKALOG="${RUN_DIR}"/kafka.log
# Start Kafka in background.
echo "Starting Kafka. Log file is ${KAFKALOG}"

_startKafka "${BASE_DIR}" "${CP}" "${KAFKALOG}" "${LOG4J_FILE}"


SPLICE_LOG="${RUN_DIR}/splice.log"
echo "Starting Master and 1 Region Server. Log file is ${SPLICE_LOG}"
# (master + region server on 1527)

HBASE_ROOT_DIR_URI="${BASE_DIR}/db/hbase"
_startSplice "${BASE_DIR}" "${SPLICE_LOG}" "${LOG4J_FILE}" "${HBASE_ROOT_DIR_URI}" "${CP}"

echo -n "  Waiting. "
until is_port_open localhost 1527; do echo -n ". " ; sleep 2; done
echo

echo "done."
popd > /dev/null
