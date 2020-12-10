#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${ROOT_DIR}/bin/functions.sh

# Start with debug logging by passing this script the "-debug" argument

SPLICELOGFILE="${ROOT_DIR}"/splice.log
ZOOLOGFILE="${ROOT_DIR}"/zoo.log
DEBUG=false

usage() {
    # $1 is an error, if any
    if [[ -n "${1}" ]]; then
        echo "Error: ${1}"
    fi
    echo "Usage: ${0} [-d] [-h]"
    echo "Where: "
    echo "  -d => Start the server with debug logging enabled"
    echo "  -h => print this message"
}

while getopts "dh" flag ; do
    case ${flag} in
        h* | \?)
            usage
            exit 0 # This is not an error, User asked help. Don't do "exit 1"
        ;;
        d)
        # start server with the debug
            DEBUG=true
        ;;
        ?)
            usage "Unknown option (ignored): ${OPTARG}"
            exit 1
        ;;
    esac
done

# server still running? - must stop first
S=$(ps -ef | awk '/SpliceTestPlatform/ && !/awk/ {print $2}')
Z=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
if [[ -e "${ROOT_DIR}"/splice_pid || -e "${ROOT_DIR}"/zoo_pid || -n ${S} || -n ${Z} ]]; then
    echo "Splice is currently running and must be shut down. Run stop-splice.sh"
    exit 1;
fi

# Must have Java installed
echo "Checking for Java..."
$(java -version >/dev/null 2>&1)
NOJAVA=$?
if [[ ${NOJAVA} -ne 0 ]]; then
    echo "Must have Java installed. Please run this script again after installing Java."
    exit 1
fi
if [[ -z $(type -p java) ]]; then
    if [[ -z "${JAVA_HOME}" ]] || [[ ! -x "${JAVA_HOME}/bin/java" ]];  then
        echo "Must have Java installed. Please run this script again after installing Java."
        exit 1
    fi
fi

# We can't run from a directory with space in the path
if [[ "${ROOT_DIR}" = *[[:space:]]* ]]; then
    echo "Please install Splice in a directory without spaces in its path:"
    echo "  ${ROOT_DIR}"
    exit 1
fi

# Config server
# Add in the lib directory where splice-site.xml resides.
CP="${ROOT_DIR}/lib":"${ROOT_DIR}/lib/*"
ZOO_DIR="${ROOT_DIR}"/db/zookeeper
HBASE_ROOT_DIR_URI="file://${ROOT_DIR}/db/hbase"

LOG4J_PATH="file:${ROOT_DIR}/lib/info-log4j.properties"
if [[ "${DEBUG}" = true ]]; then
    LOG4J_PATH="file:${ROOT_DIR}/lib/hbase-log4j.properties"
    export SPLICE_SYS_ARGS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4000"
fi

# Config for Cygwin, if necessary
if [[ ${UNAME} == CYGWIN* ]]; then
    # cygwin likes to write in 3 places for /tmp
    # we'll symlink them
    if [[ -e "/tmp" && ! -L "/tmp" ]]; then
        rm -rf "/tmp_bak"
        mv "/tmp" "/tmp_bak"
    fi
    if [[ ! -e "/cygdrive/c/tmp" ]]; then
        mkdir "/cygdrive/c/tmp"
    fi
    if [[ ! -e "/tmp" && ! -L "/tmp" ]]; then
        ln -s "/cygdrive/c/tmp" "/tmp"
    fi
    if [[ ! -e "/temp" && ! -L "/temp" ]]; then
        ln -s "/cygdrive/c/tmp" "/temp"
    fi

    # cygwin paths look a little different
	# Add in the lib directory where splice-site.xml resides.
	CP=$(cygpath --path --windows "${ROOT_DIR}/lib":"${ROOT_DIR}/lib/*")
	ZOO_DIR=$(cygpath --path --windows "${ROOT_DIR}/db/zookeeper")
    HBASE_ROOT_DIR_URI="CYGWIN"
	LOG4J_PATH="file:///$(cygpath --path --windows ${ROOT_DIR}/lib/info-log4j.properties)"
    if [[ -n "${DEBUG}" && "${DEBUG}" -eq "-debug" ]]; then
		LOG4J_PATH="file:///$(cygpath --path --windows ${ROOT_DIR}/lib/hbase-log4j.properties)"
		export SPLICE_SYS_ARGS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4000"
    fi
fi

#Remove old log files
rm -f ${SPLICELOGFILE}
rm -f ${ZOOLOGFILE}

# Start server with retry logic
ZOO_WAIT_TIME=60
SPLICE_MAIN_CLASS="com.splicemachine.test.SpliceTestPlatform"
_retrySplice "${ROOT_DIR}" "${SPLICELOGFILE}" "${ZOOLOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" ${ZOO_WAIT_TIME} "${HBASE_ROOT_DIR_URI}" "${CP}" ${SPLICE_MAIN_CLASS} "FALSE" "FALSE"
