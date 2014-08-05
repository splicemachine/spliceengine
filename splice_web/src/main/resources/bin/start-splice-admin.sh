#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${ROOT_DIR}/bin/functions-admin.sh

# Start with debug logging by passing this script the "-debug" argument

LOGFILE="${ROOT_DIR}"/splice-admin.log
DEBUG=false

usage() {
    # $1 is an error, if any
    if [[ -n "${1}" ]]; then
        echo "Error: ${1}"
    fi
    echo "Usage: ${0} [-d] [-h]"
    echo "Where: "
    echo "  -d => Start the server with debug logging enabled"
    echo "  -p <port_#> is the optional Splice Admin HTTP port to listen to.  The default HTTP port is 8080."
    echo "  -h => print this message"
}

PORT="8080"  # Default Jetty HTTP port

while getopts ":dhp:" flag ; do
    case ${flag} in
        h* | \?)
            usage
            exit 0 # This is not an error, User asked help. Don't do "exit 1"
        ;;
        p)
        # the Jetty HTTP port
            PORT=$(echo "$OPTARG" | tr -d [[:space:]])
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

echo "Running with HTTP port = ${PORT} and debug = ${DEBUG}"

# server still running? - must stop first
S=$(ps -ef | awk '/splice_web/ && !/awk/ {print $2}')
if [[ -e "${ROOT_DIR}"/splice_pid || -n ${S} ]]; then
    echo "Splice Admin is currently running and must be shut down. Run stop-splice-admin.sh"
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
    echo "Please install Splice Admin in a directory without spaces in its path:"
    echo "  ${ROOT_DIR}"
    exit 1
fi

# Config server
LOG4J_PATH="file:${ROOT_DIR}/lib/info-log4j.properties"
if [[ "${DEBUG}" = true ]]; then
    LOG4J_PATH="file:${ROOT_DIR}/lib/jetty-log4j.properties"
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
	LOG4J_PATH="file:///$(cygpath --path --windows ${ROOT_DIR}/lib/info-log4j.properties)"
    if [[ -n "${DEBUG}" && "${DEBUG}" -eq "-debug" ]]; then
		LOG4J_PATH="file:///$(cygpath --path --windows ${ROOT_DIR}/lib/jetty-log4j.properties)"
    fi
fi

# Start server with retry logic
# TODO: Remove hard coding of versioned file names.  Fetch the most recently built archives.
JETTY_RUNNER_JAR="${ROOT_DIR}/lib/jetty-runner-8.1.15.v20140411.jar"
ADMIN_MAIN_WAR="${ROOT_DIR}/lib/splice_web.war"
_retryAdmin "${ROOT_DIR}" "${LOGFILE}" "${LOG4J_PATH}" "${JETTY_RUNNER_JAR}" ${ADMIN_MAIN_WAR} ${PORT}
