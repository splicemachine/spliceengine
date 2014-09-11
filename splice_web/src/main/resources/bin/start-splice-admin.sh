#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${ROOT_DIR}/bin/functions-admin.sh

# Start with debug logging by passing this script the "-debug" argument

# Defaults
LOGFILE="${ROOT_DIR}"/splice-admin.log
DEBUG=false
CONFIG_FILE="${ROOT_DIR}"/etc/jetty-splice-admin.xml
PORT="7020"  # Default Jetty HTTP port
JDBC_ARGS="'org.apache.derby.jdbc.ClientDriver' 'databaseName=testdb;createDatabase=create' 'jdbc/mydatasource'"  # Default JDBC connect string

usage() {
    # $1 is an error, if any
    if [[ -n "${1}" ]]; then
        echo "Error: ${1}"
    fi
    echo "Usage: ${0} [-d] [-c <splice-admin.xml>] [-p <port_#>] [-j <jdbc_connect_strings>] [-h]"
    echo "Where: "
    echo "  -d => Start the server with debug logging enabled"
    echo "  -c <jetty-splice-admin.xml> is the optional Splice Admin XML configuration file.  The default is etc/jetty-splice-admin.xml."
    echo "  -p <port_#> is the optional Splice Admin HTTP port to listen to.  The default HTTP port is 7020."
    echo "  -j \"<jdbc_driver_class_name> <db_properties> <jndi_name>\" is the optional JDBC connect string."
    echo "     The default is: \"'org.apache.derby.jdbc.ClientDriver' 'databaseName=testdb;createDatabase=create' 'jdbc/mydatasource'\""
    echo "  -h => print this message"
}

while getopts ":dhc:p:j:" flag ; do
    case ${flag} in
        h* | \?)
            usage
            exit 0 # This is not an error, User asked help. Don't do "exit 1"
        ;;
        c)
        # the jetty.xml config file
            CONFIG_FILE=$(echo "$OPTARG" | tr -d [[:space:]])
        ;;
        p)
        # the Jetty HTTP port
            PORT=$(echo "$OPTARG" | tr -d [[:space:]])
        ;;
        j)
        # the JDBC classname, driver, and JNDI name
            JDBC_ARGS="${OPTARG}"
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

echo "Running with debug = ${DEBUG}, config file = ${CONFIG_FILE}, HTTP port = ${PORT}, and JDBC args = \"${JDBC_ARGS}\""

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

# Start server with retry logic and with the most recently built archive files.
JETTY_RUNNER_JAR=`ls -1r ${ROOT_DIR}/lib/jetty-runner-*.jar | head -1`
ADMIN_MAIN_WAR=`ls -1r ${ROOT_DIR}/lib/splice_web*.war | head -1`
_retryAdmin "${ROOT_DIR}" "${LOGFILE}" "${LOG4J_PATH}" "${JETTY_RUNNER_JAR}" ${ADMIN_MAIN_WAR} ${CONFIG_FILE} ${PORT} ${JDBC_ARGS}
