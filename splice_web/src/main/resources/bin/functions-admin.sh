#!/bin/bash

UNAME=$(uname -s)

_retryAdmin() {
    ROOT_DIR="${1}"
    ADMINLOGFILE="${2}"
    LOG4J_PATH="${3}"
    JETTY_RUNNER_JAR="${4}"
    ADMIN_MAIN_WAR="${5}"
    CONFIG_FILE="${6}"
    PORT="${7}"
    JDBC_ARGS="${8}"

    # Number of seconds we should allow for isReady to return 0
    # Jetty takes very little time to start up...
    ADMIN_TIMEOUT=15
    if [[ ${UNAME} == CYGWIN* ]]; then
        # Be more lenient with Cygwin
        ADMIN_TIMEOUT=30
    fi

    echo "Starting Splice Machine Admin..."
    echo "Log file is ${ADMINLOGFILE}"
    echo "Jetty Runner jar file is ${JETTY_RUNNER_JAR}"
    echo "Admin war file is ${ADMIN_MAIN_WAR}"
    echo "Waiting for Splice Admin..."

    MAXRETRY=3

    # Start Splice Admin
    RETURN_CODE=0
    ERROR_CODE=0
    for (( RETRY=1; RETRY<=MAXRETRY; RETRY++ )); do
        _startAdmin "${ROOT_DIR}" "${ADMINLOGFILE}" "${LOG4J_PATH}" "${CONFIG_FILE}" "${PORT}" "${JDBC_ARGS}"
        _waitfor "${ADMINLOGFILE}" "${ADMIN_TIMEOUT}" 'oejs.AbstractConnector:Started SelectChannelConnector'
        RETURN_CODE=$?
        if [[ ${RETURN_CODE} -eq 0 ]]; then
            ERROR_CODE=0
            break
        else
            if [[ ${RETRY} -lt ${MAXRETRY} ]]; then
                if [[ -e "${ROOT_DIR}"/admin_pid ]]; then
                    _stop "${ROOT_DIR}"/admin_pid 45
                fi
            else
                ERROR_CODE=1
            fi
        fi
    done

    if [[ ${ERROR_CODE} -ne 0 ]]; then
        if [[ -e "${ROOT_DIR}"/admin_pid ]]; then
            "${ROOT_DIR}"/bin/stop-splice-admin.sh
        fi
        echo
        echo "Splice Admin didn't start as expected. Please restart with the \"-d\" option and check ${ADMINLOGFILE}." >&2
        return 1;
    else
        echo
        echo "Splice Admin Server is ready"
        echo "The Admin URI is http://${HOSTNAME}:${PORT}/app"
        return 0;
    fi
}

_startAdmin() {
    ROOT_DIR="${1}"
    LOGFILE="${2}"
    LOG4J_PATH="${3}"
    CONFIG_FILE="${4}"
    PORT="${5}"
    JDBC_ARGS="${6}"

    ADMIN_PID_FILE="${ROOT_DIR}"/admin_pid
    export CLASSPATH
    LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_PATH"

    SYS_ARGS=" -Xmx512m -Xms256m -Djava.awt.headless=true ${LOG4J_CONFIG} -Djava.net.preferIPv4Stack=true"
    (java ${SYS_ARGS} -jar "${JETTY_RUNNER_JAR}" --config "${CONFIG_FILE}" --port "${PORT}" --jdbc "${JDBC_ARGS}" --path / "${ADMIN_MAIN_WAR}" > "${LOGFILE}" 2>&1 ) &
    echo "$!" > ${ADMIN_PID_FILE}
}

_stop() {
    PID_FILE="${1}"
    TIMEOUT="${2}"

    if [[ ! -e "${PID_FILE}" ]]; then
        echo "${PID_FILE} is not running."
        return 0;
    fi

    KILL_PID=$(grep -oE "[0-9]+" ${PID_FILE}) # Make sure we get only digits

    if [[ -n "${KILL_PID}" ]]; then

        ALIVE_PID=$(ps -p ${KILL_PID} | awk ' !/PID/ { print $1 }')
        if [[ -z "${ALIVE_PID}" || "${ALIVE_PID}" -ne "${KILL_PID}" ]]; then
            #echo "No process PID [${KILL_PID}] dead?"
            # clean up old pid file
            if [[ ${UNAME} == CYGWIN* ]]; then
				/bin/rm -f "$(cygpath --path --unix ${PID_FILE})"
            else
                /bin/rm -f "${PID_FILE}"
            fi
            return 1;
        fi

        if [[ -n "$KILL_PID" ]]; then
            kill -15 ${KILL_PID}
            sleep ${TIMEOUT}
        fi

        ALIVE_PID=$(ps -p ${KILL_PID} | awk ' !/PID/ { print $1 }')
        if [[ -n "$ALIVE_PID" ]]; then
            echo "Process didn't shut down. Trying again..."
            kill -9 ${KILL_PID}
        fi
    else
        echo "Bad PID: [${KILL_PID}] in ${PID_FILE}"
        # clean up old pid file
        if [[ ${UNAME} == CYGWIN* ]]; then
			/bin/rm -f "$(cygpath --path --unix ${PID_FILE})"
        else
            /bin/rm -f "${PID_FILE}"
        fi
        return 1;
    fi

    # clean up old pid file
    if [[ ${UNAME} == CYGWIN* ]]; then
		/bin/rm -f "$(cygpath --path --unix ${PID_FILE})"
    else
        /bin/rm -f "${PID_FILE}"
    fi
}

_stopServer() {
    ROOT_DIR="${1}"
    PID_DIR="${2}"

    echo "Shutting down Splice Admin..."
    # shut down Splice Admin, timeout value is hardcoded
    _stop "${PID_DIR}"/admin_pid 15
}

_waitfor() {
    LOGFILE="${1}"
    TIMEOUT="${2}"
    LOG_SUCCESS="${3}"
    LOG_FAILED="${4}"
    if [[ ${TIMEOUT} -le 0 ]]; then
        echo "TIMEOUT value must be positive"
        return 1;
    fi

    # number of seconds we should wait between checks on clean status
    INTERVAL=2
    # total number of seconds we should wait for clean status
    (( t = TIMEOUT ))
    while (( t > 0 )); do
        # Poll the log to check for success msg
        OLDIFS=$IFS
        IFS=$'\n'
        if [[ -n ${LOG_FAILED} ]]; then
            # Failed
			FAILED=$(grep "${LOG_FAILED}" "${LOGFILE}")
            if [ -n "${FAILED}" ]; then
                # Error in log - fail
                IFS=${OLDIFS}
                return 1;
            fi
        fi

        # Ready
		SUCCESS=$(grep "${LOG_SUCCESS}" "${LOGFILE}")
        IFS=${OLDIFS}
        if [ -n "${SUCCESS}" ]; then
            # Started, ready - success
            return 0;
        fi

        # Show something while we wait
        echo -ne "."
        sleep ${INTERVAL}

        # Still coming up... continue;
        (( t -= INTERVAL ))
    done
    # did not start in allotted timeout - error
    return 1;
}
